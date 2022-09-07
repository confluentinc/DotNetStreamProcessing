using Confluent.Kafka;

namespace TplKafka.Data;

public class Record<TKey, TValue>
{
    public TKey Key { get; }

    public TValue Value { get; }

    public Timestamp Timestamp { get; }

    public TopicPartitionOffset SourceTopicPartitionOffset { get; }

    public TopicPartition TopicPartition => SourceTopicPartitionOffset.TopicPartition;

    public Offset SourceOffset => SourceTopicPartitionOffset.Offset;

    public Record(ConsumeResult<TKey, TValue> consumeResult)
    {
        Key = consumeResult.Message.Key;
        Value = consumeResult.Message.Value;
        SourceTopicPartitionOffset = consumeResult.TopicPartitionOffset;
        Timestamp = consumeResult.Message.Timestamp;
    }

    public Record(TKey key, 
        TValue value, 
        Timestamp timestamp, 
        TopicPartitionOffset sourceTopicPartitionOffset)
    {
        Key = key;
        Value = value;
        Timestamp = timestamp;
        SourceTopicPartitionOffset = sourceTopicPartitionOffset;
    }

    public override string ToString()
    {
        return SourceTopicPartitionOffset+ ": "+Key + ": " + Value;
    }
    
}