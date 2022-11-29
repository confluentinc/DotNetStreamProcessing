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

This repository contains the code for the [Building Event Streaming Applications in .NET
](#) blog 
that combines the [Kafka .NET Clients](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#ak-dotnet) - 
[Producer](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#ak-dotnet) and [Consumer](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#consumer)
with the [Dataflow (TPL Dataflow Library)](https://learn.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library) 
for building an example event stream-processing application.

Note that application presented here is just **_an example_** of what you could possibly
build with the .NET clients the TPL Dataflow library - feel free to experiment by using different Dataflow blocks, your own functions etc.

### Running the application

Here are the steps for running the application






