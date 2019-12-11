# Kafka Connect Connector

A generic Kafka Connect source provides ability to plug any Kafka Connect source for data ingestion to Jet pipelines.

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  No   |
| Distributed |  No   |

## Getting Started

### Installing

The Kafka Connect Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>kafka-connect</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'kafka-connect', version: ${version}
```

### Usage

To use the any Kafka Connect Connector as a source in your pipeline you need to create a 
source with a call to `KafkaConnectSources.connect()` method with the `Properties` object. 
After that you can use your pipeline like any other source in the
Jet pipeline. The source will emit items in `SourceRecord` type from Kafka 
Connect API, where you can access the key and value along with their corresponding
schemas.

Following is an example pipeline which stream events from RabbitMQ, maps the values to
their string representation and and logs them.

Beware the fact that you'll need to attach the Kafka Connect Connector of your 
choice with the job that you are submitting.

```java
Properties properties = new Properties();
properties.setProperty("name", "rabbitmq-source-connector");
properties.setProperty("connector.class", "com.github.jcustenborder.kafka.connect.rabbitmq.RabbitMQSourceConnector");
properties.setProperty("kafka.topic", "messages");
properties.setProperty("rabbitmq.queue", "test-queue");
properties.setProperty("rabbitmq.host", "???");
properties.setProperty("rabbitmq.port", "???");
properties.setProperty("rabbitmq.username", "???");
properties.setProperty("rabbitmq.password", "???");

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(KafkaConnectSources.connect(properties))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/kafka-connect-rabbitmq-0.0.2-SNAPSHOT.zip");

Job job = createJetMember().newJob(pipeline, jobConfig);
job.join();
```

The pipeline will output records like the following:

```
INFO: [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] Output to ordinal 0: 
{
   "consumerTag":"amq.ctag-06l2oPQOnzjaGlAocCTzwg",
   "envelope":{
      "deliveryTag":100,
      "isRedeliver":false,
      "exchange":"ex",
      "routingKey":"test"
   },
   "basicProperties":{
      "contentType":"text/plain",
      "contentEncoding":"UTF-8",
      "headers":{

      },
      "deliveryMode":null,
      "priority":null,
      "correlationId":null,
      "replyTo":null,
      "expiration":null,
      "messageId":null,
      "timestamp":null,
      "type":null,
      "userId":"guest",
      "appId":null
   },
   "body":"Hello World!"
}
```
P.S. The record has been pretty printed for clarity.

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
