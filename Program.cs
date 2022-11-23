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
    /// <summary>
    /// Main class used to run the Kafka TPL Dataflow integration
    /// NOTE: You have to pass in a path to a config file used for connecting to Kafka.
    /// It's assumed that you'll use the properties similar to those generated in the client section of
    /// Confluent Cloud.  Also it's expecting data in JSON format from the Confluent Datagen
    /// Source Connector with the Purchases schema
    /// </summary>
    static class TplKafkaStreaming
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Logger.Error("Must provide the path for the properties file for connection");
                return;
            }

            var path = args[0];
            var consumerConfig = ClientUtils.ConsumerConfig(path);
            var producerConfig = ClientUtils.ProducerConfig(path);

            consumerConfig.EnableAutoCommit = true;
            consumerConfig.EnableAutoOffsetStore = false;
            consumerConfig.AutoCommitIntervalMs = 30_000;
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.ClientId = "tplConsumer";
            consumerConfig.GroupId = "tplConsumerGroup";
            producerConfig.ClientId = "tplProducer";
            
            var cancellationToken = new CancellationTokenSource();

            var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
            var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
            var commitObserver = new OffsetHandler(consumer);

            var desFunc = ProcessorFunctions<string, string>.DeserializeFunc(Deserializers.Utf8, Deserializers.Utf8);
            var protoSerFunc = ProcessorFunctions<string, Purchase>.SerializeProtoFunc(Serializers.Utf8);
            var mappingFunc = ProcessorFunctions<string, string>.MapPurchase();
            var timeConsumingTaskFunc = ProcessorFunctions<string, Purchase>.SimulatedLongTimeService();

            var linkOptions = new DataflowLinkOptions {PropagateCompletion = true};
            var parallelizationBlockOptions = new ExecutionDataflowBlockOptions()
                {BoundedCapacity = 10_000, MaxDegreeOfParallelism = 4};
            var parallelizationAnyOrderBlockOptions = new ExecutionDataflowBlockOptions()
                {BoundedCapacity = 10_000, MaxDegreeOfParallelism = 4, EnsureOrdered = false};
            var standardBlockOptions = new ExecutionDataflowBlockOptions() {BoundedCapacity = 10_000};

            var deserializeBlock =
                new TransformBlock<Record<byte[], byte[]>, Record<string, string>>(desFunc,
                    standardBlockOptions);
            var serializeBlock =
                new TransformBlock<Record<string, Purchase>, Record<byte[], byte[]>>(protoSerFunc,
                    standardBlockOptions);
            var mapToPurchaseBlock =
                new TransformBlock<Record<string, string>, Record<string, Purchase>>(mappingFunc, standardBlockOptions);
            var simulatedLongTimeTaskBlock =
                new TransformBlock<Record<string, Purchase>, Record<string, Purchase>>(timeConsumingTaskFunc,
                    parallelizationBlockOptions);
            
            KafkaSourceBlock sourceBlock = new(consumer, "tpl_input", cancellationToken, 100_000);
            Logger.Info("Starting the source block");
            sourceBlock.Start();

            KafkaSinkBlock sinkBlock = new(producer, "tpl_output", commitObserver, cancellationToken);
            Logger.Info("Starting the sink block");
            sinkBlock.Start();

            sourceBlock.LinkTo(deserializeBlock, linkOptions);
            deserializeBlock.LinkTo(mapToPurchaseBlock, linkOptions);
            mapToPurchaseBlock.LinkTo(simulatedLongTimeTaskBlock, linkOptions);
            simulatedLongTimeTaskBlock.LinkTo(serializeBlock, linkOptions);
            serializeBlock.LinkTo(sinkBlock, linkOptions);
            Logger.Info($"Start processing records at {DateTime.Now}");
            Console.WriteLine("Hit any key to quit the program");
            Console.ReadKey();
            cancellationToken.Cancel();
            sourceBlock.Complete();
        }
    }
}