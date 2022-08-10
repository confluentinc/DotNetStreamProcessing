// See https://aka.ms/new-console-template for more information



using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Io.Confluent.Developer.Proto;
using TplKafka.Client;
using TplKafka.Data;
using TplKafka.Processors;
using TplKafka.Sink;
using TplKafka.Source;
using DataflowLinkOptions = System.Threading.Tasks.Dataflow.DataflowLinkOptions;


namespace TplKafka
{
    class TplKafkaStreaming
    {
        static void Main(string[] args)
        {
            
             var path = "/Users/bill/dev/github_clones/tpl-kafka/TplKafka/confluent-props.txt";
             var consumerConfig = ClientUtils.ConsumerConfig(path);
             var producerConfig = ClientUtils.ProducerConfig(path);
            
             consumerConfig.EnableAutoCommit = false;
             consumerConfig.GroupId = "tpl-consumer-groupY";

             var  consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
             var  producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
             var commitObserver = new CommitObserver(consumer);
             
             var desFunc = ProcessorFunctions<string, string>.DeserializeFunc(Deserializers.Utf8, Deserializers.Utf8);
             var protoSerFunc = ProcessorFunctions<string, Purchase>.SerializeProtoFunc(Serializers.Utf8);
             var mappingFunc = ProcessorFunctions<string, string>.MapPurchase();
             
             var linkOptions = new DataflowLinkOptions  {PropagateCompletion = true};
             var parallelizationBlockOptions = new ExecutionDataflowBlockOptions()
                 {BoundedCapacity = 100, MaxDegreeOfParallelism = 4};
             var standardBlockOptions = new ExecutionDataflowBlockOptions() {BoundedCapacity = 100};

             var deserializeBlock = new TransformBlock<Record<byte[], byte[]>, Record<string, string>>(desFunc, parallelizationBlockOptions);
             var serializeBlock = new TransformBlock<Record<string, Purchase>, Record<byte[], byte[]>>(protoSerFunc, parallelizationBlockOptions);
             var mapToPurchaseBlock = new TransformBlock<Record<string, string>, Record<string, Purchase>>(mappingFunc, standardBlockOptions);
             
             var cancellationToken = new CancellationTokenSource();
             
             KafkaSourceBlock sourceBlock = new(consumer, "tpl-input", cancellationToken);
             Console.WriteLine("Starting the source block");
             sourceBlock.Start();

             Console.WriteLine("Source block is running! press any key to stop");
             KafkaSinkBlock sinkBlock = new(producer, "tpl-output", commitObserver, cancellationToken);
             sinkBlock.Start();
            
             sourceBlock.LinkTo(deserializeBlock,linkOptions);
             deserializeBlock.LinkTo(mapToPurchaseBlock, linkOptions);
             mapToPurchaseBlock.LinkTo(serializeBlock, linkOptions, input => input.Value.Quantity > 1);
             mapToPurchaseBlock.LinkTo(DataflowBlock.NullTarget<Record<string, Purchase>>());
             serializeBlock.LinkTo(sinkBlock, linkOptions);
             Console.WriteLine("Hit any key to quit the program");
             Console.ReadKey();
             cancellationToken.Cancel();
             sourceBlock.Complete();
             
        }
    }

}

