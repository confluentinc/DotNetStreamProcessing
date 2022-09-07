using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Client;
using TplKafka.Data;

namespace TplKafka.Source;
/// <summary>
/// The InOrderKafkaSourceBlock forwards records grouped by TopicPartition ordered by timestamp
/// The behavior is to collect records and group them by TopicPartition stored in a List.
/// Once a time threshold is reached (based on the timestamps of the records) the lists
/// of TopicPartition records are ordered by timestamp based on the record at the head of each list then
/// forwarded through the pipeline.
/// 
/// By default, blocks forward records in the order the block received them
/// even if the parallelism for block is greater than one
/// <see cref="https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/how-to-specify-the-degree-of-parallelism-in-a-dataflow-block#robust-programming"/>
/// enable.auto.commit should be set to "true" and enable.auto.offset.store should be set to "false" <see cref="OffsetHandler"/>
/// After a successful produce request the original offset for the record is passed to the OffsetHandler
/// and the Consumer will commit the offsets stored there in the next auto-commit interval 
/// </summary>

public class InOrderKafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationToken;
    private const int SendInterval = 30_000;
    private long _currentTimestamp;
    private long _lastSendTimestamp;
    private long _recordsConsumed;
    private bool _wasBlocked;
    private const int RetryBackoff = 2000;
    private readonly Dictionary<TopicPartition, List<Record<byte[], byte[]>>>_recordMap = new();
    private readonly PriorityQueue<List<Record<byte[], byte[]>>, long> _byTimestampQueue = new();

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
                // We need to keep records grouped by TopicPartition to ensure in-order processing
                if (_recordMap.ContainsKey(record.TopicPartition))
                {
                    _recordMap[record.TopicPartition].Add(record);
                }
                else
                {
                    _recordMap.Add(record.TopicPartition, new List<Record<byte[], byte[]>>{record});
                }
                // Make sure we have enough to process
                if (_currentTimestamp - _lastSendTimestamp <= SendInterval) continue;
                Logger.Info($"Time to send buffered reccords {_currentTimestamp} less {_lastSendTimestamp}");
                // Need to make sure of all the lists in the map we start processing records with smallest timestamp
                foreach (var records in _recordMap.Values)
                {
                    _byTimestampQueue.Enqueue(records, records[0].Timestamp.UnixTimestampMs);
                }

                while (_byTimestampQueue.Count > 0)
                {
                    var records = _byTimestampQueue.Dequeue();
                    records.ForEach(incomingRecord =>
                    {
                        while (!_messageBuffer.Post(incomingRecord))
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
                    });
                    records.Clear();
                }

                _lastSendTimestamp = _currentTimestamp;
            } 
            Logger.Info("Dropping out of consume loop");
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