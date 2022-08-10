
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;


namespace TplKafka.Source;

public class KafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationToken;

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
        Task.Factory.StartNew(async () =>
        {
            _consumer.Subscribe(_topic);
            while (!_cancellationToken.IsCancellationRequested)
            { 
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(TimeSpan.FromSeconds(5));
                if (consumeResult != null)
                {
                    Record<byte[], byte[]> record = new Record<byte[], byte[]>(consumeResult);
                    Console.WriteLine("Consumed a record " + record);

                    await _messageBuffer.SendAsync(record);
                }
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