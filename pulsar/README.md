# Apache Pulsar Connector

A Hazelcast Jet connector for Apache Pulsar which enables Hazelcast Jet pipelines to read/write from/to Pulsar topics.

## Connector Attributes
### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |   No  |
| Stream      |  Yes  |
| Distributed |   No  |

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
## Usage

Describe the module usage and how it interacts with the rest of the system. The
entry point of the module must be included in this section like `InfluxDbSinks.influxDb()`.

End with a very small example/snippet of getting some data out of the system 
or using it for a little demo

## Fault-Tolerance
Describe F-T behavior, give information about whether the source is replayable, 
talk about checkpointing and transactional reads. For sinks describe idempotence 
and/or transactional writes.

## Running the tests

Explain how to run the automated tests for this system

```
./gradlew test
```

## Authors

* **Ufuk Yılmaz**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details
