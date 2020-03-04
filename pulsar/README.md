# Apache Pulsar Connector

A Hazelcast Jet connector for Apache Pulsar which enables Hazelcast Jet pipelines to read/write from/to Pulsar topics.

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
| Distributed |   No  |


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

#### Reading from Pulsar topic

In practice, using distributed stream source like this:
```java
import com.hazelcast.jet.contrib.pulsar.*;
[...]
StreamSource<String> pulsarSource = PulsarSources.pulsarDistributed(
          Collections.singletonList(topicName),
          2,       // Preferred Number of Local Parallelism
          consumerConfig,
          () -> PulsarClient.builder()
                            .serviceUrl("pulsar://exampleserviceurl")
                            .build(), // Client Supplier
          () -> Schema.BYTES, // Schema Supplier Function
          x -> new String(x.getData()) // Projection function that converts
                                       // receiving bytes to String before
                                       // emitting.
          );
pipeline.readFrom(pulsarSource)
        .writeTo(Sinks.logger()); 
```
## Authors

* **Ufuk Yılmaz**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details
