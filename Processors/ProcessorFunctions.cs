using System.Security.Cryptography;
using Confluent.Kafka;
using Google.Protobuf;
using Io.Confluent.Developer.Proto;
using Newtonsoft.Json;
using NLog;
using TplKafka.Data;

namespace TplKafka.Processors;
/// <summary>
/// Class containing the various functions that a
/// Dataflow block uses to accomplish its task
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public static class ProcessorFunctions<TKey, TValue>
{
    private static readonly Random  _randomNumberGenerator = Random.Shared;
    private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
    public static Func<Record<byte[], byte[]>, Record<TKey, TValue>> DeserializeFunc(
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
    {
        return (record) =>
        {
            try
            {
                var desKey = keyDeserializer.Deserialize(record.Key, record.Key == null, SerializationContext.Empty);
                var desValue =
                    valueDeserializer.Deserialize(record.Value, record.Value == null, SerializationContext.Empty);
                return new Record<TKey, TValue>(desKey, desValue, record.Timestamp, record.SourceTopicPartitionOffset);
            }
            catch (Exception e)
            {
                Logger.Error($"Error in the deserialization process {e} with record {record}");
                throw new Exception("Error deserialization", e);
            }
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
            return new Record<byte[], byte[]>(serKey, serValue, record.Timestamp, record.SourceTopicPartitionOffset);
        };
    }
    /// <summary>
    /// Only have the key serializer specified as we're relying on Protobuf's toByteArray functionality
    /// to serialize the value
    /// </summary>
    /// <param name="keySerializer"></param>
    /// <returns></returns>
    public static Func<Record<TKey, Purchase>, Record<byte[], byte[]>> SerializeProtoFunc(
        ISerializer<TKey> keySerializer)
    {
        return (input) =>
        {
            var serKey = keySerializer.Serialize(input.Key, SerializationContext.Empty);
            var serValue = input.Value.ToByteArray();
            return new Record<byte[], byte[]>(serKey, serValue, input.Timestamp, input.SourceTopicPartitionOffset);
        };
    }

    public static Func<Record<string, string>, Record<string, Purchase>> MapPurchase()
    {
        RandomNumberGenerator.Create();
        return (input) =>
        {
            var jsonPurchase = JsonConvert.DeserializeObject<Dictionary<string, object>>(input.Value) ?? new Dictionary<string, object>
            {
                ["id"] = -1,
                ["item_type"] = "null record",
                ["quantity"] = -1

            };
            var purchase = new Purchase()
            {
                Id = (long) jsonPurchase["id"],
                Item = (string) jsonPurchase["item_type"],
                Quantity = (long) jsonPurchase["quantity"],
                PricePerUnit = _randomNumberGenerator.Next(10, 51)
            };
            return new Record<string, Purchase>(input.Key, purchase, input.Timestamp, input.SourceTopicPartitionOffset);
        };
    }
  /// <summary>
  /// This Func represents a simulated remote service call
  /// </summary>
  /// <returns></returns>
    public static Func<Record<string, Purchase>, Record<string, Purchase>> SimulatedLongTimeService()
    {
        return (input) =>
        {
            Thread.Sleep(_randomNumberGenerator.Next(1, 4));
            return input;
        };
    }

}