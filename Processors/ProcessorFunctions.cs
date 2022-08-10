using System.Numerics;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Io.Confluent.Developer.Proto;
using Newtonsoft.Json;
using TplKafka.Data;
using System.Text;
using Google.Protobuf;

namespace TplKafka.Processors;

public static class ProcessorFunctions<TKey, TValue>
{
    public static Func<Record<byte[], byte[]>, Record<TKey, TValue>> DeserializeFunc(
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        return (record) =>
        {
            var desKey = keyDeserializer.Deserialize(record.Key, record.Key == null, SerializationContext.Empty);
            var desValue =
                valueDeserializer.Deserialize(record.Value, record.Value == null, SerializationContext.Empty);
            return new Record<TKey, TValue>(desKey, desValue, record.Timestamp, record.TopicPartitionOffset);
        };
    }

    public static Func<Record<TKey, TValue>, Record<byte[], byte[]>> SerializeFunc(ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        return (record) =>
        {
            var serKey = keySerializer.Serialize(record.Key, SerializationContext.Empty);
            var serValue =
                valueSerializer.Serialize(record.Value, SerializationContext.Empty);
            return new Record<byte[], byte[]>(serKey, serValue, record.Timestamp, record.TopicPartitionOffset);
        };
    }

    public static Func<Record<TKey, Purchase>, Record<byte[], byte[]>> SerializeProtoFunc(
        ISerializer<TKey> keySerializer)
    {
        return (input) =>
        {
            var serKey = keySerializer.Serialize(input.Key, SerializationContext.Empty);
            var serValue = input.Value.ToByteArray();
            return new Record<byte[], byte[]>(serKey, serValue, input.Timestamp, input.TopicPartitionOffset);
        };
    }

    public static Func<Record<string, string>, Record<string, Purchase>> MapPurchase()
    {
        RandomNumberGenerator.Create();
        return (input) =>
        {
            var jsonPurchase = JsonConvert.DeserializeObject<Dictionary<string, object>>(input.Value);
            var purchase = new Purchase()
            {
                Id = (long) jsonPurchase["id"],
                Item = (string) jsonPurchase["item_type"],
                Quantity = (long) jsonPurchase["quantity"],
                PricePerUnit = RandomNumberGenerator.GetInt32(2, 50)
            };
            return new Record<string, Purchase>(input.Key, purchase, input.Timestamp, input.TopicPartitionOffset);
        };
    }
    
}