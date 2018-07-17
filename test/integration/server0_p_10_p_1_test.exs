defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_p_1

  @tag :createtopic
  test "can create a topic" do
    KafkaEx.create_topics("ploup")
    topics = KafkaEx.metadata.topic_metadatas |> Enum.map(&(&1.topic))
    IO.inspect(topics)
    assert Enum.member?(topics, "ploup")
  end
end
