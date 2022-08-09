using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Sink;

public class KafkaSinkBlock : ITargetBlock<Record<byte[], byte[]>>
{
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IProducer<byte[], byte[]> _producer;
    private bool _keepProducing = true;
    private readonly string _outputTopic;
    private readonly  IObserver<Record<byte[],byte[]>>_commitObserver;

    public KafkaSinkBlock(IProducer<byte[], byte[]> producer, string outputTopic,
        IObserver<Record<byte[], byte[]>> commitObserver)
    {
        _producer = producer;
        _outputTopic = outputTopic;
        _commitObserver = commitObserver;
    }

    public void Start()
    {
        Task.Factory.StartNew(() =>
        {
            while (_keepProducing)
            {
                Record<byte[], byte[]> record =  _messageBuffer.Receive();
                Console.WriteLine("Record from source block " + record);
                Action<DeliveryReport<byte[], byte[]>> handler = r =>
                {
                    if (!r.Error.IsError)
                        _commitObserver.OnNext(record);
                    else
                        Console.WriteLine( $"Delivery Error: {r.Error.Reason}");
                };
                   
                _producer.Produce(_outputTopic, new Message<byte[], byte[]>{ Key = record.Key, Value = record.Value}, handler);
            }
        });
    }

    public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, Record<byte[], byte[]> messageValue, ISourceBlock<Record<byte[], byte[]>>? source,
        bool consumeToAccept)
    {
         return ((ITargetBlock<Record<byte[],byte[]>>)_messageBuffer).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
         
    }

    public void Complete()
    {
        Console.WriteLine("Complete on the SinkBlock called");
        _keepProducing = false;
        _producer.Flush();
        _producer.Dispose();
        _messageBuffer.Complete();
    }

    public void Fault(Exception exception)
    {
        ((IDataflowBlock) _messageBuffer).Fault(exception);
    }

    public Task Completion 
    {
        get
        {
            _keepProducing = false;
            _producer.Flush();
            _producer.Dispose();
            return _messageBuffer.Completion;
        }
        
    }
        
}