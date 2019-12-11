# Debezium Connector

A Hazelcast Jet connector for [Debezium](https://debezium.io/) which enables Hazelcast Jet pipelines to consume CDC events from various databases.

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  No   |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  No   |
| Distributed |  No   |

## Getting Started

### Installing

The Debezium Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>debezium</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'debezium', version: ${version}
```

### Usage

To use the Debezium Connectors as a source in your pipeline you need to create a 
source with a call to `DebeziumSource.cdc()` method with the `Configuration` object
from Debezium. After that you can use your pipeline like any other source in the
Jet pipeline. The source will emit items in `SourceRecord` type from Kafka 
Connect API, where you can access the key and value along with their corresponding
schemas.

Following is an example pipeline which stream events from MySQL, maps the values to
their string representation and and logs them.

Beware the fact that you'll need to attach the JARs of Debezium Connector of your 
choice with the job that you are submitting.

```java
Configuration configuration = Configuration
        .create()
        .with("name", "mysql-inventory-connector")
        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        .with("database.hostname", "mysqlhostname")
        .with("database.port", "3306")
        .with("database.user", "debezium")
        .with("database.password", "dbz")
        .with("database.server.id", "184054")
        .with("database.server.name", "dbserver1")
        .with("database.whitelist", "inventory")
        .with("database.history.hazelcast.list.name", "test")
        .build();


Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/debezium-connector-mysql.zip");

JetInstance jet = createJetMember();
Job job = jet.newJob(pipeline, jobConfig);
job.join();
```

The pipeline will output records like the following

```
10:59:37,773  INFO || - [map#1] hz.amazing_antonelli.jet.cooperative.thread-3 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] Output to ordinal 0: 
{
   "before":null,
   "after":{
      "id":108,
      "name":"jacket",
      "description":"water resistent black wind breaker",
      "weight":0.10000000149011612
   },
   "source":{
      "version":"1.0.0.Beta3",
      "connector":"mysql",
      "name":"dbserver1",
      "ts_ms":0,
      "snapshot":"true",
      "db":"inventory",
      "table":"products",
      "server_id":0,
      "gtid":null,
      "file":"mysql-bin.000003",
      "pos":515,
      "row":0,
      "thread":null,
      "query":null
   },
   "op":"c",
   "ts_ms":1574931577694
}
```
P.S. The record has been pretty printed for clarity.

Check out tests folder for integration tests which covers the same scenario for various databases.


### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
