defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_p_1

  @tag :createtopic
  test "can create a topic" do
    name = "topic3_#{:rand.uniform(2000000)}"

    request = %{
      topic: name,
      num_partitions: 10,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: [
        %{config_name: "cleanup.policy", config_value: "compact"},
        %{config_name: "min.compaction.lag.ms", config_value: "0"}
      ]}

    resp = KafkaEx.create_topics([request])
    # error = NONE
    assert {0, name} == parse_create_topic_resp(resp)

    resp = KafkaEx.create_topics([request])
    # error = TOPIC_ALREADY_EXISTS
    assert {36, name} == parse_create_topic_resp(resp)

    topics = KafkaEx.metadata.topic_metadatas |> Enum.map(&(&1.topic))
    assert Enum.member?(topics, name)
  end

  def parse_create_topic_resp(response) do
    %KafkaEx.Protocol.CreateTopics.Response{
      topic_errors: [
        %KafkaEx.Protocol.CreateTopics.TopicError{
          error_code: error_code,
          topic_name: topic_name
        }
      ]} = response
    {error_code, topic_name}
  end
end
