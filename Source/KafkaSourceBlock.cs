
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using NLog.Fluent;
using TplKafka.Data;


namespace TplKafka.Source;
/// <summary>
/// The source for a Dataflow TPL topology of blocks
/// The order of processing follows the order of records as consumed
/// enable.auto.commit should be set to "true" and enable.auto.offset.store should be set to "false" <see cref="TplKafka.Client.OffsetHandler"/>
/// After a successful produce request the original offset for the record is passed to the OffsetHandler
/// and the Consumer will commit the offsets manually stored using Consumer.StoreOffset 
/// </summary>

public class KafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationToken;
    private readonly int _consumeReportInterval;
    private const int RetryBackoff = 2000;
    private long _recordsConsumed;
    private bool _wasBlocked;

    public KafkaSourceBlock(IConsumer<byte[], byte[]> consumer, 
        string topic, 
        CancellationTokenSource cancellationToken,
        int consumeReportInterval)
    {
        _consumer = consumer;
        _topic = topic;
        _cancellationToken = cancellationToken;
        _consumeReportInterval = consumeReportInterval;
        var sourceBlockOptions = new ExecutionDataflowBlockOptions() {BoundedCapacity = 10000};
        _messageBuffer = new BufferBlock<Record<byte[], byte[]>>(sourceBlockOptions);
    }

    public void Start()
    {
        Task.Factory.StartNew( () =>
        {
            _consumer.Subscribe(_topic);
            Logger.Info("Subscribed to topic");
            while (!_cancellationToken.IsCancellationRequested)
            {
                var consumeResult = new ConsumeResult<byte[], byte[]>();
                try
                {
                    consumeResult = _consumer.Consume(TimeSpan.FromSeconds(5));
                }
                catch (ConsumeException exception)
                {
                    Logger.Error($"Error consuming {exception}");
                    Logger.Info("Cancelling now");
                    _cancellationToken.Cancel();
                }
                if (consumeResult == null) continue;
                var record = new Record<byte[], byte[]>(consumeResult);
                // Note that if the time spent in this section 
                // exceeds the max.poll.timeout this consumer 
                // will get removed from the group and a rebalance will occur
                while (!_messageBuffer.Post(record))
                {
                    Logger.Warn("message buffer full, blocking until available");
                    _wasBlocked = true;
                    Thread.Sleep(RetryBackoff);
                }

                if (_wasBlocked)
                {
                    Logger.Info("message buffer accepting records again");
                    _wasBlocked = false;
                }

                if (++_recordsConsumed % _consumeReportInterval == 0)
                {
                    Logger.Info($"{_recordsConsumed} records consumed so far");
                }
            }
            Logger.Info("Dropping out of consume loop");
        });
    }

    public void Complete()
    {
       Logger.Info("Complete on the SourceBlock called");
        _cancellationToken.Cancel();
        _consumer.Close();
        _consumer.Dispose();
        _messageBuffer.Complete();
    }

    public void Fault(Exception exception)
    {
        ((ISourceBlock<Record<byte[], byte[]>>) _messageBuffer).Fault(exception);
    }

    public Task Completion
    {
        get
        {
            _consumer.Close();
            _consumer.Dispose();
            return _messageBuffer.Completion;
        }
    }

    public Record<byte[], byte[]>? ConsumeMessage(DataflowMessageHeader messageHeader,
        ITargetBlock<Record<byte[], byte[]>> target, out bool messageConsumed)
    {
        return ((ISourceBlock<Record<byte[], byte[]>>) _messageBuffer).ConsumeMessage(messageHeader, target,
            out messageConsumed);
    }

    public IDisposable LinkTo(ITargetBlock<Record<byte[], byte[]>> target, DataflowLinkOptions linkOptions)
    {
        return _messageBuffer.LinkTo(target, linkOptions);
    }

    public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<Record<byte[], byte[]>> target)
    {
        ((ISourceBlock<Record<byte[], byte[]>>) _messageBuffer).ReleaseReservation(messageHeader, target);
    }

    public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<Record<byte[], byte[]>> target)
    {
        return ((ISourceBlock<Record<byte[], byte[]>>) _messageBuffer).ReserveMessage(messageHeader, target);
    }
}