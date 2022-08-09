using Confluent.Kafka;

namespace TplKafka.Data;

public class Record<TKey, TValue>
{
    public TKey Key { get; }

    public TValue Value { get; }

    public Timestamp Timestamp { get; }
    
    
    public TopicPartitionOffset TopicPartitionOffset { get; }
    

    public Record(ConsumeResult<TKey, TValue> consumeResult)
    {
        Key = consumeResult.Message.Key;
        Value = consumeResult.Message.Value;
        TopicPartitionOffset = consumeResult.TopicPartitionOffset;
        Timestamp = consumeResult.Message.Timestamp;
    }

    public Record(TKey key, 
        TValue value, 
        Timestamp timestamp, 
        TopicPartitionOffset topicPartitionOffset)
    {
        Key = key;
        Value = value;
        Timestamp = timestamp;
        TopicPartitionOffset = topicPartitionOffset;
    }

    public override string ToString()
    {
        return TopicPartitionOffset+ ": "+Key + ": " + Value;
    }
    
}