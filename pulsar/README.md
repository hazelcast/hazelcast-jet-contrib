# Apache Pulsar Connector

A Hazelcast Jet connector for Apache Pulsar which enables Hazelcast Jet
pipelines to read/write from/to Pulsar topics.

It uses the Pulsar client library to read and publish messages from/to a
Pulsar topic. Pulsar client library provides two different ways of
reading messages from a topic. They are namely Consumer API and Reader
API that have major differences. Pulsar connectors have benefits of both
Consumer API and Reader API. Both the sources and sink creation APIs of
the Pulsar connector implement the builder pattern. We get the required
parameters in the constructor and other parameters in the setters.
Configuration values are set to default values and these default values
can be changed using setters. We setted readerName, consumerName,
subscriptionName, batchReceivePolicy to their default values, but they
can be changed by using the configuration setters. Since the batch
receive policy of the consumer is more likely to change, a separate
builder setter is created for it.

 The Pulsar Consumer Source is a distributed source that is created
using the Consumer abstraction of the Pulsar. The Consumer API has a
subscription mechanism which is used to consume messages from the first
unacknowledged message of this subscription. The consumer source uses
shared (or round-robin) subscription mode. We create one consumer client
per source processor of Hazelcast Jet, and these allow consuming
messages in a distributed manner. This source consumes messages in a
blocking batch manner, and the batch receive policy can be configurable
in the builder methods. The distributed manner prevents the order of
messages from being preserved. Since the Pulsar Consumer API does not
return the cursor for the last consumed message, it is incapable of
assuring fault-tolerance.

 The Pulsar Reader Source is a fault-tolerant source that provides the
exactly-once processing guarantee. In Pulsar Reader Source, the
MessageId of the latest read message is stored in the snapshot. In case
of failure, the job restarts from the latest snapshot and reads from the
stored MessageId. Since it requires a partition-mapping logic, this
source is not a distributed source.

The Consumer Source is faster than the Reader Source because the
Consumer source is a distributed source. But, the Reader Source is
better than the Consumer in terms of processing guarantee it provides.

Besides, this Pulsar client library has an API called the Producer API.
The Pulsar connector enables users to use the Pulsar topic as a sink in
Jet pipelines.

Note that: The Pulsar sources and sink do not fail fast in the scenario
that when the job reading Pulsar topic using the Pulsar consumer source
is running and Pulsar broker is closed, the job doesn't fail, it just
prints log at warn level. The reason for this is that the Pulsar Client
tries to reconnect to the broker with exponential backoff in this case,
and while doing this, we cannot notice the connection issue because it
only prints logs without throwing an exception.

For more detailed information about the design of this connector
[look](https://jet-start.sh/docs/design-docs/009-pulsar-connector)

## Connector Attributes

### Source Attributes

|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |   No  |
| Stream      |  Yes  |
| Distributed |  Yes  |

### Sink Attributes

|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

## Getting Started

### Installing

The Hazelcast Apache Pulsar Connector artifacts are published on the
Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to
your project:

```xml
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>pulsar</artifactId>
    <version>${version}</version>
</dependency>
```

If you are using Gradle:

```gradle
compile group: 'com.hazelcast.jet.contrib', name: 'pulsar', version: ${version}
```

## Running the tests

To run the tests run the command below:

```sh
./gradlew test
```

### Usage

#### Reading from a Pulsar topic

##### Using Pulsar Reader Source

We have tutorial for the Pulsar reader source
[here](https://jet-start.sh/docs/tutorials/pulsar).

##### Using Pulsar Consumer Source

In practice, using Pulsar consumer stream source like this:

```java
import com.hazelcast.jet.contrib.pulsar.*;
[...]
StreamSource<String> pulsarReaderSource = PulsarSources.pulsarConsumerBuilder(
            "topicName",
            () -> PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build(),
            () -> Schema.BYTES, /* Schema Supplier Function */
            x -> new String(x.getData(), StandardCharsets.UTF_8) /*
                           Projection function that converts receiving bytes
                           to String before emitting. */
            ).build();
pipeline.readFrom(pulsarSource)
        .writeTo(Sinks.logger());
```

### Publish Messages to a Pulsar topic

The pipeline below connects the local Pulsar cluster located at
`localhost:6650`(default address) and then publishes messages to the
pulsar topic, `hazelcast-demo-topic`,

```java
import com.hazelcast.jet.contrib.pulsar.*;
[...]
Sink<Integer> pulsarSink = PulsarSinks.builder("hazelcast-demo-topic",
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

See also the list of
[contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors)
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the
[LICENSE](../LICENSE) file for details
