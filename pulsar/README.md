# Apache Pulsar Connector

A Hazelcast Jet connector for Apache Pulsar which enables Hazelcast Jet 
pipelines to read/write from/to Pulsar topics.

It uses the Pulsar client library to read and publish messages from/to a
Pulsar topic. Pulsar client library provides two different ways of
reading messages from a topic. They are namely Consumer API and Reader
API that have major differences. Pulsar connectors have benefits of both
Consumer API and Reader API.

Besides, this Pulsar client library has an API called the Producer API.
The Pulsar connector enables users to use the Pulsar topic as a sink in
Jet pipelines.
## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |   No  |
| Stream      |  Yes  |
| Distributed |   Yes  |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |


## Getting Started

### Installing

The Hazelcast Apache Pulsar Connector artifacts are published on the Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>pulsar</artifactId>
    <version>${version}</version>
</dependency>
```
If you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'pulsar', version: ${version}
```
## Running the tests

To run the tests run the command below: 

```
./gradlew test
```

### Usage

#### Reading from a Pulsar topic

##### Using Pulsar Reader Source
We have tutorial for the Pulsar reader source [here](https://jet-start.sh/docs/tutorials/cdc).

##### Using Pulsar Consumer Source

In practice, using Pulsar consumer stream source like this:
```java
import com.hazelcast.jet.contrib.pulsar.*;
[...]
StreamSource<String> pulsarSource = PulsarSources.pulsarConsumer(
          Collections.singletonList(topicName),
          2, /* Preferred Number of Local Parallelism */
          consumerConfig,
          () -> PulsarClient.builder()
                            .serviceUrl("pulsar://exampleserviceurl")
                            .build(), /* Pulsar Client Supplier */
          () -> Schema.BYTES, /* Schema Supplier Function */
          () -> BatchReceivePolicy.builder()
                                  .maxNumMessages(512)
                                  .timeout(1000, TimeUnit.MILLISECONDS)
                                  .build(), /* Batch Receive Policy Supplier */
          x -> new String(x.getData(), StandardCharsets.UTF_8) /*
               Projection function that converts receiving bytes
               to String before emitting. */
          );
pipeline.readFrom(pulsarSource)
        .writeTo(Sinks.logger()); 
```

### Publish Messages to a Pulsar topic

The pipeline below connects the local Pulsar cluster located at 
`localhost:6650`(default address) and then publishes messages
to the pulsar topic, `hazelcast-demo-topic`,

```java
import com.hazelcast.jet.contrib.pulsar.*;
[...]
Map<String, Object> producerConfig = new HashMap<>();
Sink<Integer> pulsarSink = PulsarSinks.builder("hazelcast-demo-topic",
        producerConfig,
        () -> PulsarClient.builder()
                          .serviceUrl("pulsar://localhost:6650")
                          .build(),
        () -> Schema.INT32,
        FunctionEx.identity()).build();
p.readFrom(TestSources.itemStream(15))
 .withoutTimestamps()
 .map(x -> (int) x.sequence())
 .writeTo(pulsarSink);

```

## Authors

* **Ufuk Yılmaz**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](../LICENSE) 
file for details
