
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;


namespace TplKafka.Source;
/// <summary>
/// The source for a Dataflow TPL topology of blocks
/// The order of processing follows the order of records as consumed
/// i.e there is no ordering of records.
/// For ordered processing by topic-partition <see cref="InOrderKafkaSourceBlock"/>
/// enable.auto.commit should be set to "true" and enable.auto.offset.store should be set to "false" <see cref="OffsetHandler"/>
/// After a successful produce request the original offset for the record is passed to the OffsetHandler
/// and the Consumer will commit the offsets stored there in the next auto-commit interval 
/// </summary>

public class KafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationToken;
    private const int RetryBackoff = 2000;
    private long _recordsConsumed;
    private bool _wasBlocked;

    public KafkaSourceBlock(IConsumer<byte[], byte[]> consumer, 
        string topic, 
        CancellationTokenSource cancellationToken)
    {
        _consumer = consumer;
        _topic = topic;
        _cancellationToken = cancellationToken;
    }

    public void Start()
    {
        Task.Factory.StartNew( () =>
        {
            _consumer.Subscribe(_topic);
            while (!_cancellationToken.IsCancellationRequested)
            { 
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(5));
                if (consumeResult == null) continue;
                var record = new Record<byte[], byte[]>(consumeResult);

                while (!_messageBuffer.Post(record))
                {
                    Logger.Debug("message buffer full, blocking until available");
                    _wasBlocked = true;
                    Thread.Sleep(RetryBackoff);
                }

                if (_wasBlocked)
                {
                    Logger.Info("message buffer accepting records again");
                    _wasBlocked = false;
                }

                if (_recordsConsumed++ % 1000 == 0)
                {
                    Logger.Info($"{_recordsConsumed} total number of records consumed so far");
                }
            }
            Logger.Info("Dropping out of consume loop");
        });
    }

    public void Complete()
    {
        Console.WriteLine("Complete on the SourceBlock called");
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