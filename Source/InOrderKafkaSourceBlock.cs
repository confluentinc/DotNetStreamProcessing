using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Source;
/// <summary>
/// The InOrderKafkaSourceBlock forwards records grouped by TopicPartition.
/// By default, blocks forward records in the order the block received them
/// even if the parallelism for block is greater than one <see cref="https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/how-to-specify-the-degree-of-parallelism-in-a-dataflow-block#robust-programming"/>
/// AutoCommit should be set to "false".  The <see cref="TplKafka.Client.CommitObserver"/>
/// will commit after a successful produce request. 
/// </summary>

public class InOrderKafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationToken;
    private const int SendInterval = 30_000;
    private long _currentTimestamp;
    private long _lastSendTimestamp;
    private readonly Dictionary<TopicPartition, List<Record<byte[], byte[]>>>_recordMap = new();

    public InOrderKafkaSourceBlock(IConsumer<byte[], byte[]> consumer, 
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
                
                _currentTimestamp = consumeResult.Message.Timestamp.UnixTimestampMs;
                if (_lastSendTimestamp == 0) _lastSendTimestamp = _currentTimestamp;
                var record = new Record<byte[], byte[]>(consumeResult);
                //TODO Update this to also sort by offset ascending
                if (_recordMap.ContainsKey(record.TopicPartition))
                {
                    _recordMap[record.TopicPartition].Add(record);
                }
                else
                {
                    _recordMap.Add(record.TopicPartition, new List<Record<byte[], byte[]>>{record});
                }

                if (_currentTimestamp - _lastSendTimestamp <= SendInterval) continue;
                foreach (var records in _recordMap.Values)
                {
                    Console.WriteLine($"Sending records in order for {records[0].TopicPartition}");
                    records.ForEach( r => _messageBuffer.Post(r));
                    records.Clear();
                }

                _lastSendTimestamp = _currentTimestamp;
            }
            Console.WriteLine("Dropping out of consume loop");
        });
    }

    public void Complete()
    {
        Console.WriteLine("Complete on the SourceBlock called");
        _cancellationToken.Cancel();
        _consumer.Close();
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