using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Sink;
/// <summary>
/// The <see cref="KafkaSinkBlock"/> is the final node in the Dataflow topology.
/// It wraps a <see cref="IProducer{TKey,TValue}"/> for producing back to Kafka
/// and uses a <see cref="BufferBlock{T}"/> as a delegate for the Dataflow block.
/// </summary>
public class KafkaSinkBlock : ITargetBlock<Record<byte[], byte[]>>
{
    private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
    private readonly BufferBlock<Record<byte[], byte[]>> _messageBuffer = new();
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _outputTopic;
    private readonly  IObserver<Record<byte[],byte[]>>_commitObserver;
    private readonly CancellationTokenSource _cancellationToken;

    public KafkaSinkBlock(IProducer<byte[], byte[]> producer, string outputTopic,
        IObserver<Record<byte[], byte[]>> commitObserver,
        CancellationTokenSource cancellationToken)
    {
        _producer = producer;
        _outputTopic = outputTopic;
        _commitObserver = commitObserver;
        _cancellationToken = cancellationToken;
    }

    public void Start()
    {
        Task.Factory.StartNew( () =>
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                Record<byte[], byte[]> record = _messageBuffer.Receive(_cancellationToken.Token);
                // The handler is used to pass a successfully produced record to the CommitObserver
                Action<DeliveryReport<byte[], byte[]>> handler = r =>
                {
                    if (!r.Error.IsError)
                        _commitObserver.OnNext(record);
                    else
                        Logger.Error( $"Delivery Error: {r.Error.Reason}");
                };
                
                _producer.Produce(_outputTopic, new Message<byte[], byte[]>{ Key = record.Key, Value = record.Value}, handler);
            }
            Logger.Info("Dropping out of the produce loop");
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
            _producer.Flush();
            _producer.Dispose();
            return _messageBuffer.Completion;
        }
        
    }
        
}