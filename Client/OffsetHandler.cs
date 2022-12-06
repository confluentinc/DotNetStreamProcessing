using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Client;

/// <summary>
/// The OffsetHandler is a wrapper around the IConsumer used in one of the source blocks
/// <see cref="TplKafka.Source.KafkaSourceBlock"/> the
/// offset of the source record is preserved during the entire process of the pipeline and once the record is
/// successfully processed (acked) the offset is given to the IConsumer.StoreOffset(tpOffset) method
/// and the Consumer will commit the offsets already processed - this should allow for at-least once processing guarantees 
/// </summary>  
public class OffsetHandler : IObserver<Record<byte[], byte[]>>
{
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly int _offsetsStoredReportInterval;
    private long _offsetStored;
    private Dictionary<int, long> _offsetTracker = new();


    public OffsetHandler(IConsumer<byte[], byte[]> consumer,
        int offsetsStoredReportInterval)
    {
        _consumer = consumer;
        _offsetsStoredReportInterval = offsetsStoredReportInterval;
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
        
        var tpOffset = new TopicPartitionOffset(processedRecord.TopicPartition, processedRecord.SourceOffset + 1);
        _consumer.StoreOffset(tpOffset);
        _offsetTracker[tpOffset.Partition.Value] = tpOffset.Offset.Value;
        if (++_offsetStored % _offsetsStoredReportInterval == 0)
        {
            Logger.Info($"{_offsetStored} records successfully processed and corresponding offsets committed");
            var offsetReport = _offsetTracker.Aggregate("", (current, kv) => 
                current + $"partition:{kv.Key}->offset[{kv.Value}], ");
            Logger.Info($"{ offsetReport.TrimEnd(',', ' ')}");
        }
    }
}