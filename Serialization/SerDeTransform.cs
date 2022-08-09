using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Serialization;

public static class SerDeTransform<TKey, TValue>
{

    public static Func<Record<byte[], byte[]>, Record<TKey, TValue>> DeserializeFunc(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer )
     {
         return (record) =>
         {
             var desKey = keyDeserializer.Deserialize(record.Key, record.Key == null, SerializationContext.Empty);
             var desValue =
                 valueDeserializer.Deserialize(record.Value, record.Value == null, SerializationContext.Empty);
             return new Record<TKey, TValue>(desKey, desValue, record.Timestamp, record.TopicPartitionOffset);
         };
     }
    
    public static Func<Record<TKey, TValue>, Record<byte[], byte[]>> SerializeFunc(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer )
    {
        return (record) =>
        {
            var serKey = keySerializer.Serialize(record.Key, SerializationContext.Empty);
            var serValue =
                valueSerializer.Serialize(record.Value, SerializationContext.Empty);
            return new Record<byte[], byte[]>(serKey, serValue, record.Timestamp, record.TopicPartitionOffset);
        };
    }
}