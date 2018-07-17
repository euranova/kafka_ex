defmodule KafkaEx.Server0P10P1 do
  @moduledoc """
  Implements kafkaEx.Server behaviors for kafka 0.10.1 API.
  """
  use KafkaEx.Server
  alias KafkaEx.Protocol.CreateTopics
  alias KafkaEx.Server0P8P2
  alias KafkaEx.Server0P9P0

  require Logger

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], [name: name])
  end

  # The functions below are all defined in KafkaEx.Server0P8P2
  defdelegate kafka_server_consumer_group(state), to: Server0P8P2
  defdelegate kafka_server_fetch(fetch_request, state), to: Server0P8P2
  defdelegate kafka_server_offset_fetch(offset_fetch, state), to: Server0P8P2
  defdelegate kafka_server_offset_commit(offset_commit_request, state), to: Server0P8P2
  defdelegate kafka_server_consumer_group_metadata(state), to: Server0P8P2
  defdelegate kafka_server_update_consumer_metadata(state), to: Server0P8P2

  # The functions below are all defined in KafkaEx.Server0P9P0
  defdelegate kafka_server_init(args), to: Server0P9P0
  defdelegate kafka_server_join_group(request, network_timeout, state_in), to: Server0P9P0
  defdelegate kafka_server_sync_group(request, network_timeout, state_in), to: Server0P9P0
  defdelegate kafka_server_leave_group(request, network_timeout, state_in), to: Server0P9P0
  defdelegate kafka_server_heartbeat(request, network_timeout, state_in), to: Server0P9P0
  defdelegate consumer_group?(state), to: Server0P9P0

  # CreateTopics Request (Version: 0) => [create_topic_requests] timeout
  # create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
  #   topic => STRING
  #   num_partitions => INT32
  #   replication_factor => INT16
  #   replica_assignment => partition [replicas]
  #     partition => INT32
  #     replicas => INT32
  #   config_entries => config_name config_value
  #     config_name => STRING
  #     config_value => NULLABLE_STRING
  # timeout => INT32

  def kafka_create_topics(topic_name, state) do
    create_topics_request = %{
      create_topic_requests: [%{
        topic: topic_name,
        num_partitions: 3,
        replication_factor: 2,
        replica_assignment: [],
        config_entries: []
      }],
      timeout: 2000

    }
    mainRequest = CreateTopics.create_request(state.correlation_id, @client_id, create_topics_request)

    # unless consumer_group?(state) do
    #   raise ConsumerGroupRequiredError, offset_fetch
    # end

    {broker, state} = KafkaEx.Server0P9P0.broker_for_consumer_group_with_update(state)

    # # if the request is for a specific consumer group, use that
    # # otherwise use the worker's consumer group
    # consumer_group = offset_fetch.consumer_group || state.consumer_group
    # offset_fetch = %{offset_fetch | consumer_group: consumer_group}

    # offset_fetch_request = OffsetFetch.create_request(state.correlation_id, @client_id, offset_fetch)

    {response, state} = case broker do
      nil    ->
        Logger.log(:error, "Coordinator for topic is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
          |> NetworkClient.send_sync_request(mainRequest, config_sync_timeout())
          |> case do
               {:error, reason} -> {:error, reason}
               response -> response
             end
        {response, %{state | correlation_id: state.correlation_id + 1}}
    end
    IO.inspect("Response: ")
    IO.inspect(response)
    {:reply, response, state}
  end

end
