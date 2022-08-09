
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;


namespace TplKafka.Source;

public class KafkaSourceBlock : ISourceBlock<Record<byte[], byte[]>>
{
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IConsumer<byte[], byte[]> _consumer;
    private bool _keepConsuming = true;
    private readonly string _topic;

    public KafkaSourceBlock(IConsumer<byte[], byte[]> consumer, string topic)
    {
        _consumer = consumer;
        _topic = topic;
    }

    public void Start()
    {
        Task.Factory.StartNew(() =>
        {
            _consumer.Subscribe(_topic);
            while (_keepConsuming)
            { 
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(TimeSpan.FromSeconds(5));
                Record<byte[], byte[]> record = new Record<byte[], byte[]>(consumeResult);
                Console.WriteLine("Consumed a record " + record);

                _messageBuffer.Post(record);
            }
        });
    }

    public void Complete()
    {
        Console.WriteLine("Complete on the SourceBlock called");
        _keepConsuming = false;
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
            _keepConsuming = false;
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
        Console.WriteLine("SourceBlock now linked to " + target);
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