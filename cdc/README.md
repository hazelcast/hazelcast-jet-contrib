# Debezium Connector

[Change data capture (CDC)](https://en.wikipedia.org/wiki/Change_data_capture)
is a set of software design patterns used to determine (and track) the
data that has changed so that action can be taken using the changed
data. CDC is also an approach to data integration that is based on the
identification, capture and delivery of the changes made to enterprise
data sources.

[Debezium](https://debezium.io/) is an open source distributed platform
for change data capture. Start it up, point it at your databases, and
your apps can start responding to all of the inserts, updates, and
deletes that other apps commit to your databases. According to the [Debezium website](https://debezium.io/documentation/faq/#what_are_some_uses_of_debezium)
, the primary use of Debezium is to enable applications to respond
almost immediately whenever data in databases change. Applications can
do anything with the insert, update, and delete events. They might use
the events to know when to remove entries from a cache. They might
update search indexes with the data. They might update a derived data
store with the same information or with information computed from the
changing data, such as with Command Query Responsibility Separation
(CQRS). They might send a push notification to one or more mobile
devices. They might aggregate the changes and produce a stream of
patches for entities

This module includes a Hazelcast Jet connector for Debezium which
enables Hazelcast Jet pipelines to consume change events from various
databases supported by Debezium.

Here is the list of stable Debezium connectors verified to work with
Hazelcast Jet Pipelines:

- [MySQL Connector](https://debezium.io/documentation/reference/1.0/connectors/mysql.html)
- [MongoDB Connector](https://debezium.io/documentation/reference/1.0/connectors/mongodb.html)
- [PostgreSQL Connector](https://debezium.io/documentation/reference/1.0/connectors/postgresql.html)
- [MS SQL Server Connector](https://debezium.io/documentation/reference/1.0/connectors/sqlserver.html)

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

The Debezium Connector artifacts are published on the Maven
repositories.

Add the following lines to your pom.xml to include it as a dependency to
your project:

```xml
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

To use the Debezium Connectors as a source in your pipeline you need to
create a source with a call to `DebeziumSource.cdc()` method with the
`Configuration` object from Debezium. After that you can use your
pipeline like any other source in the Jet pipeline. The source will emit
items in `SourceRecord` type from Kafka Connect API, where you can
access the key and value along with their corresponding schemas.

Following is an example pipeline which stream events from MySQL from the
`customers` table, maps the events to `Event` POJO and filters out only
update events and logs them.

Beware the fact that you'll need to attach the JARs of Debezium
Connector of your choice with the job that you are submitting. The
Debezium Connectors can be downloaded from [this link](https://debezium.io/releases/1.0/)
.

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
        .with("table.whitelist", "inventory.customers")
        .with("include.schema.changes", "false")
        .with("database.history.hazelcast.list.name", "test")
        .build();


Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .filterUsingService(ServiceFactories.sharedService(context -> {
            Serde<Event> serde = DebeziumSerdes.payloadJson(Event.class);
            serde.configure(Collections.emptyMap(), false);
            return serde.deserializer();
         }, Deserializer::close), (deserializer, record) -> {
            String recordString = Values.convertToString(record.valueSchema(), record.value())
            EventRecord eventRecord = deserializer.deserialize("", recordString.getBytes());
            return eventRecord.isUpdate();
         })
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/debezium-connector-mysql.zip");

JetInstance jet = createJetMember();
Job job = jet.newJob(pipeline, jobConfig);
job.join();
```

The Event class used to build the deserializer is quite simple:

```java
 private static final class Event implements Serializable {

        @JsonIgnore
        public JsonNode source;

        @JsonIgnore
        public JsonNode after;

        @JsonIgnore
        public JsonNode before;

        @JsonIgnore
        public long ts_ms;

        @JsonProperty
        public String op;

        public Event() {
        }

        public Event(String operation) {
            this.op = operation;
        }

        public boolean isUpdate() {
            return "u".equals(op);
        }

        public boolean isCreate() {
            return "c".equals(op);
        }
    }
```

The pipeline will output records like the following, you can find
explanations for the field [here](https://debezium.io/documentation/reference/1.0/connectors/mysql.html#change-events-value)

```log
10:59:37,773  INFO || - [map#1] hz.amazing_antonelli.jet.cooperative.thread-3 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] Output to ordinal 0:
{
  "before": {
    "id": 1004,
    "first_name": "Anne",
    "last_name": "Kretchmar",
    "email": "annek@noanswer.org"
  },
  "after": {
    "id": 1004,
    "first_name": "Anne Marie",
    "last_name": "Kretchmar",
    "email": "annek@noanswer.org"
  },
  "source": {
    "version": "1.0.0.Beta3",
    "connector": "mysql",
    "name": "dbserver1",
    "ts_ms": 1582884308000,
    "snapshot": "false",
    "db": "inventory",
    "table": "customers",
    "server_id": 223344,
    "gtid": null,
    "file": "mysql-bin.000003",
    "pos": 364,
    "row": 0,
    "thread": 6,
    "query": null
  },
  "op": "u",
  "ts_ms": 1582884308456
}
```

P.S. The record has been pretty printed for clarity.

Check out [tests
 folder](src/test/java/com/hazelcast/jet/contrib/debezium) for
 integration tests which covers the same scenario for various databases.

### Fault-Tolerance

The Debezium connectors driven by Jet are participating to store their
state snapshots (e.g partition offsets + any metadata which they might
have to recover/restart) in Jet. This way when the job is restarted they
can recover their state and continue to consume from where they left
off. Since Debezium itself consists of multiple Kafka Connect modules,
each will have different behaviors when there is a failure. Please refer
to the corresponding [Debezium
Connectors](https://debezium.io/documentation/reference/1.0/connectors/index.html)
page for more detailed information.

### Running the tests

To run the tests run the command below:

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
