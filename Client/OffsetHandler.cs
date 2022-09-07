using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Client;

/// <summary>
/// The OffsetHandler is a wrapper around the IConsumer used in one of the source blocks
/// <see cref="TplKafka.Source.KafkaSourceBlock"/> or <see cref="TplKafka.Source.InOrderKafkaSourceBlock"/> the
/// offset of the source record is preserved during the entire process of the pipeline and once the record is
/// successfully processed (acked) the offset is given to the IConsumer.StoreOffset(tpOffset) method
/// and the Consumer will commit the offsets already processed - this should allow for at-least once processing guarantees 
/// </summary>  
public class OffsetHandler : IObserver<Record<byte[], byte[]>>
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    private long _offsetStored;

    public OffsetHandler(IConsumer<byte[], byte[]> consumer)
    {
        _consumer = consumer;
    }

    public void OnCompleted()
    {
        throw new NotImplementedException();
    }

    public void OnError(Exception error)
    {
        throw new NotImplementedException();
    }

    public void OnNext(Record<byte[], byte[]> processedRecord)
    {
        if (Logger.IsTraceEnabled)
        {
            Logger.Trace($"Record for storing offset {processedRecord.SourceTopicPartitionOffset}");
        }

        if (_offsetStored++ % 1000 == 0)
        {
            Logger.Info($"{_offsetStored} number of records have been successfully processed and offsets stored");
        }

        var tpOffset = new TopicPartitionOffset(processedRecord.TopicPartition, processedRecord.SourceOffset + 1);

        _consumer.StoreOffset(tpOffset);
    }
}