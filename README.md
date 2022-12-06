### What is Stream Processing

Stream processing is an approach to software development that 
views events as an application's primary input or output.  
Stream processing is becoming the de facto approach for dealing with event 
data.  For example, there's no sense in waiting to act on information or 
responding to a potential fraudulent credit card purchase.  
Other times it might involve handling an incoming flow of records in a 
microservice, and processing them most efficiently is best for your application.  
Whatever the use case, it's safe to say that an event streaming approach is 
the best approach for handling events.

### About this repository

![.NET Streaming with Kafka](img/kafka_application_graph_flow.excalidraw.png)

This repository contains the code for the [Building Event Streaming Applications in .NET
blog](#) (blog will be published around December 8th, I'll update with a link then)
that combines the [Kafka .NET Clients](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#ak-dotnet) - 
[Producer](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#ak-dotnet) and [Consumer](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#consumer)
with the [Dataflow (TPL Dataflow Library)](https://learn.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library) 
for building an example event stream-processing application.

Note that application presented here is just **_an example_** of what you could possibly
build with the .NET clients the TPL Dataflow library - feel free to experiment by using different Dataflow blocks, your own functions etc.

### Prerequisites

* [.NET version 6.0 (or greater)](https://dotnet.microsoft.com/en-us/download)
* A user account in [Confluent Cloud](https://www.confluent.io/confluent-cloud/tryfree/)
* Local installation of [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) v2.34.0 or later
* [jq](https://stedolan.github.io/jq/download/) - Used for the script to set-up Confluent Cloud
* [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) **Windows only** The application was developed on macOS BigSur 11.2


You can use the CLI to create a Confluent Cloud account (new accounts get $400 free for the first 30 days).
Run the following command and follow the prompts:
```text
confluent cloud-signup
```

Then run `confluent login --save` to log into your account before moving on.

### Running the application

You'll need the following resources set up in Confluent Cloud
1. Kafka Cluster
2. Input and output topics (`tpl_input` and `tpl_output` respectively)
3. API Key and secret for connection to the cluster
4. A [Datagen Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#quick-start) to provide input records for the application.

There is a script [build-cluster.sh](build-cluster.sh) that will handle all the details of everything you need to set up for running the application. The script will generate
a properties file `kafka.properties` used for connecting to Confluent Cloud. Git will ignore this file but it's important to make sure you don't accidentally check this file 
in as it contains your cloud credentials.

The build script defaults to using `gcp` and `us-east1` for the cloud provider and region respectively. 
To use a different cloud provider and region, `aws` and `us-west-2` for example, execute the following before running the script:
```text
export CLOUD=aws
export REGION=us-west-2
```

To run the application execute the following commands:
1. `./build-cluster.sh`
2. `dotnet clean && dotnet build`
3. `dotnet run kafka.properties`

After executing `dotnet run...` you should see log output similar to this:
```text
2022-11-30 19:11:30.8839 INFO TplKafka.TplKafkaStreaming.Main-90: Starting the source block
2022-11-30 19:11:30.9498 INFO TplKafka.TplKafkaStreaming.Main-94: Starting the sink block
2022-11-30 19:11:30.9515 INFO TplKafka.TplKafkaStreaming.Main-102: Start processing records at 11/30/2022 7:11:30 PM
Hit any key to quit the program
2022-11-30 19:11:30.9525 INFO TplKafka.Source.KafkaSourceBlock.Start-46: Subscribed to topic
2022-11-30 19:11:31.8507 INFO TplKafka.TplKafkaStreaming.Main-58: consumer tplConsumer-92cf31b0-ad53-4f9e-9a57-360841095d91 had partitions tpl_input [[0]],tpl_input [[1]],tpl_input [[2]],tpl_input [[3]],tpl_input [[4]],tpl_input [[5]] assigned
```

To stop the application from running just hit any key.

### Cleaning up

This application uses real resources on Confluent Cloud so it's important to clean everything up when done to avoid any unnecessary charges.
There is another script [teardown.sh](teardown.sh) that will clean everything up that you created in the previous step.


Execute the following command to remove the cluster, topics, and the datagen connector.
```text
./teardown.sh
```

### Application details

This application simulates a workflow where the input events are a `Purchase` object.  It uses a [Quick Start Datagen Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#datagen-source-connector-for-ccloud) to provide
the input records with the following structure:
```json
{
  "id": "long",
  "item_type": "string",
  "quantity": "long"
}
```

The [purchase-datagen.json](purchase-datagen.json) file included in the project defines the required parameters for starting the datagen source connector.  The connector will produce
a record approximately every 10 milliseconds.  To change the rate of records produced into the `tpl_input` topic you can update the `max.interval` field 
on the `purchase-datagen.json` file to the desired value.

Note that `build-cluster.sh` script generates a temporary `json` file for creating the connector via the command line.  This is done because an API key and secret are required
and those values should never get committed into GitHub. 








