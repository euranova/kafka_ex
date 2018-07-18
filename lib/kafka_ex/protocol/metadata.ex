defmodule KafkaEx.Protocol.Metadata do
  alias KafkaEx.Protocol
  import KafkaEx.Protocol.Common

  @default_api_version 0

  @moduledoc """
  Implementation of the Kafka Hearbeat request and response APIs
  """
  defmodule Request do
    @moduledoc false
    defstruct topic: nil
    @type t :: %Request{topic: binary}
  end

  defmodule Broker do
    @moduledoc false

    alias KafkaEx.Socket

    defstruct node_id: -1, host: "", port: 0, socket: nil
    @type t :: %__MODULE__{}

    def connected?(%Broker{} = broker) do
      broker.socket != nil && Socket.open?(broker.socket)
    end
  end

  defmodule Response do
    @moduledoc false
    alias KafkaEx.Protocol.Metadata.Broker
    alias KafkaEx.Protocol.Metadata.TopicMetadata
    defstruct brokers: [], topic_metadatas: [], controller_id: nil
    @type t :: %Response{
      brokers: [Broker.t],
      topic_metadatas: [TopicMetadata.t],
      controller_id: integer
    }

    def broker_for_topic(metadata, brokers, topic, partition) do
      case Enum.find(metadata.topic_metadatas, &(topic == &1.topic)) do
        nil -> nil
        topic_metadata -> find_lead_broker(metadata.brokers, topic_metadata, brokers, partition)
      end
    end

    def partitions_for_topic(metadata, topic) do
      case Enum.find(metadata.topic_metadatas, &(&1.topic == topic)) do
        nil ->
          []  # topic doesn't exist yet, no partitions
        topic_metadata ->
          Enum.map(topic_metadata.partition_metadatas, &(&1.partition_id))
      end
    end

    defp find_lead_broker(metadata_brokers, topic_metadata, brokers, partition) do
      case Enum.find(topic_metadata.partition_metadatas, &(partition == &1.partition_id)) do
        nil -> nil
        lead_broker -> find_broker(lead_broker, metadata_brokers, brokers)
      end
    end

    defp find_broker(lead_broker, metadata_brokers, brokers) do
      case Enum.find(metadata_brokers, &(lead_broker.leader == &1.node_id)) do
        nil -> nil
        broker ->
          Enum.find(brokers, &broker_for_host?(&1, broker.host, broker.port))
      end
    end

    defp broker_for_host?(broker, host, port) do
      broker.host == host && broker.port == port && Broker.connected?(broker)
    end
  end

  defmodule TopicMetadata do
    @moduledoc false
    alias KafkaEx.Protocol.Metadata.PartitionMetadata
    defstruct error_code: 0, topic: nil, partition_metadatas: []
    @type t :: %TopicMetadata{
      error_code: integer | :no_error,
      topic: nil | binary,
      partition_metadatas: [PartitionMetadata.t]
    }
  end

  defmodule PartitionMetadata do
    @moduledoc false
    defstruct error_code: 0, partition_id: nil, leader: -1, replicas: [], isrs: []
    @type t :: %PartitionMetadata{
      error_code: integer,
      partition_id: nil | integer,
      leader: integer,
      replicas: [integer],
      isrs: [integer]
    }
  end

  def valid_api_version(v) do
    case v do
      nil -> @default_api_version
      v -> v
    end
  end

  def create_request(correlation_id, client_id, ""), do: create_request(correlation_id, client_id, "", nil)
  def create_request(correlation_id, client_id, "", api_version) do
    version = valid_api_version(api_version)
    IO.inspect("*********  create_request A version #{version}")
    topic_count = if 0 == version, do: 0, else: -1
    KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id, version) <> << topic_count :: 32-signed >>
  end

  def create_request(correlation_id, client_id, topic, api_version) when is_binary(topic), do: create_request(correlation_id, client_id, [topic], valid_api_version(api_version))

  def create_request(correlation_id, client_id, topics, api_version) when is_list(topics) do
    IO.inspect("********* create_request B version #{valid_api_version(api_version)}")
    KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id, valid_api_version(api_version)) <> << length(topics) :: 32-signed, topic_data(topics) :: binary >>
  end

  def parse_response(data) do
    parse_response(data, nil)
  end

  def parse_response(<< _correlation_id :: 32-signed, brokers_size :: 32-signed, rest :: binary >>, api_version) do
    version = valid_api_version(api_version)
    IO.inspect("******** parse_response *********** version #{version}")
    case version do
      1 ->
        {brokers, rest} = parse_brokers_v1(brokers_size, rest, [])
        << controller_id :: 32-signed, rest :: binary >> = rest
        << topic_metadatas_size :: 32-signed, rest :: binary >> = rest
        IO.inspect("topics metadata size #{topic_metadatas_size}")
        %Response{brokers: brokers, controller_id: controller_id, topic_metadatas: parse_topic_metadatas(topic_metadatas_size, rest)}
      0 ->
        {brokers, rest} = parse_brokers(brokers_size, rest, [])
        << topic_metadatas_size :: 32-signed, rest :: binary >> = rest
        %Response{brokers: brokers, topic_metadatas: parse_topic_metadatas(topic_metadatas_size, rest)}
    end
  end

  defp parse_brokers(0, rest, brokers), do: {brokers, rest}

  defp parse_brokers(brokers_size, << node_id :: 32-signed, host_len :: 16-signed, host :: size(host_len)-binary, port :: 32-signed, rest :: binary >>, brokers) do
    parse_brokers(brokers_size - 1, rest, [%Broker{node_id: node_id, host: host, port: port} | brokers])
  end

  defp parse_brokers_v1(0, rest, brokers), do: {brokers, rest}

  defp parse_brokers_v1(brokers_size, <<
                                      node_id :: 32-signed,
                                      host_len :: 16-signed,
                                      host :: size(host_len)-binary,
                                      port :: 32-signed,
                                      # rack is nullable
                                      -1 :: 16-signed,
                                      rest :: binary
                                    >>, brokers) do
    IO.inspect("              parse_brokers null RACK")
    parse_brokers_v1(brokers_size - 1, rest, [%Broker{node_id: node_id, host: host, port: port} | brokers])
  end

  defp parse_brokers_v1(brokers_size, <<
                                      node_id :: 32-signed,
                                      host_len :: 16-signed,
                                      host :: size(host_len)-binary,
                                      port :: 32-signed,
                                      rack_len :: 16-signed,
                                      rack :: size(rack_len)-binary,
                                      rest :: binary
                                    >>, brokers) do
    IO.inspect("              parse_brokers with RACK")
    IO.inspect({node_id, host_len, host, port, rack_len, rack})
    parse_brokers_v1(brokers_size - 1, rest, [%Broker{node_id: node_id, host: host, port: port} | brokers])
  end

  defp parse_topic_metadatas(0, _), do: []

  defp parse_topic_metadatas(topic_metadatas_size, << error_code :: 16-signed, topic_len :: 16-signed, topic :: size(topic_len)-binary, partition_metadatas_size :: 32-signed, rest :: binary >>) do
    {partition_metadatas, rest} = parse_partition_metadatas(partition_metadatas_size, [], rest)
    [%TopicMetadata{error_code: Protocol.error(error_code), topic: topic, partition_metadatas: partition_metadatas} | parse_topic_metadatas(topic_metadatas_size - 1, rest)]
  end

  defp parse_partition_metadatas(0, partition_metadatas, rest), do: {partition_metadatas, rest}

  defp parse_partition_metadatas(partition_metadatas_size, partition_metadatas, << error_code :: 16-signed, partition_id :: 32-signed, leader :: 32-signed, rest :: binary >>) do
    {replicas, rest} =  parse_replicas(rest)
    {isrs, rest} =  parse_isrs(rest)
    parse_partition_metadatas(partition_metadatas_size - 1, [%PartitionMetadata{error_code: Protocol.error(error_code), partition_id: partition_id, leader: leader, replicas: replicas, isrs: isrs} | partition_metadatas], rest)
  end

  defp parse_replicas(<< num_replicas :: 32-signed, rest :: binary >>) do
    parse_int32_array(num_replicas, rest)
  end

  defp parse_isrs(<< num_isrs :: 32-signed, rest ::binary >>) do
    parse_int32_array([], num_isrs, rest)
  end

  defp parse_int32_array(array \\ [], num, data)

  defp parse_int32_array(array, 0, rest) do
    {Enum.reverse(array), rest}
  end

  defp parse_int32_array(array, num, << value :: 32-signed, rest :: binary >>) do
    parse_int32_array([value|array], num - 1, rest)
  end
end
