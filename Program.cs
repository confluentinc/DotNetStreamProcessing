// See https://aka.ms/new-console-template for more information



using Confluent.Kafka;
using TplKafka.Client;
using TplKafka.Data;
using TplKafka.Serialization;
using TplKafka.Sink;
using TplKafka.Source;
using DataflowLinkOptions = System.Threading.Tasks.Dataflow.DataflowLinkOptions;


namespace TplKafka
{
    class TplKafkaStreaming
    {
        static void Main(string[] args)
        {
            DataflowLinkOptions linkOptions = new DataflowLinkOptions  {PropagateCompletion = true};
             var path = "/Users/bill/dev/github_clones/tpl-kafka/TplKafka/confluent-props.txt";
             var consumerConfig = ClientUtils.ConsumerConfig(path);
             var producerConfig = ClientUtils.ProducerConfig(path);

             consumerConfig.EnableAutoCommit = false;
             consumerConfig.GroupId = "tpl-group";

             var  consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
             var  producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
             var commitObserver = new CommitObserver(consumer);

             var desFunc = SerDeTransform<string, string>.DeserializeFunc(Deserializers.Utf8, Deserializers.Utf8);
             var serFunc = SerDeTransform<string, string>.SerializeFunc(Serializers.Utf8, Serializers.Utf8);

             var deserializeBlock =
                 new System.Threading.Tasks.Dataflow.TransformBlock<Record<byte[], byte[]>, Record<string, string>>(desFunc);
             var serializeBlock =
                 new System.Threading.Tasks.Dataflow.TransformBlock<Record<string, string>, Record<byte[], byte[]>>(serFunc);

             KafkaSourceBlock sourceBlock = new(consumer, "tpl-input");
             Console.WriteLine("Starting the source block");
             sourceBlock.Start();
             
             
             
             Console.WriteLine("Source block is running! press any key to stop");
             KafkaSinkBlock sinkBlock = new(producer, "tpl-output", commitObserver);
             sinkBlock.Start();
             Console.WriteLine("SinkBlock started");
             sourceBlock.LinkTo(deserializeBlock,linkOptions);
             deserializeBlock.LinkTo(serializeBlock, linkOptions);
             serializeBlock.LinkTo(sinkBlock, linkOptions);
             Console.WriteLine("Link called from Program");
             Console.ReadKey();
             
             sourceBlock.Complete();
        }
    }

}

