# MongoDB Connector

A Hazelcast Jet connector for MongoDB which enables Hazelcast Jet pipelines to 
read/write data points from/to MongoDB.

## Connector Attributes

### Source Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

## Getting Started

### Installing

The MongoDB Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your
project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>mongodb</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'mongodb', version: ${version}
```

## Usage

### As a Batch Source

MongoDB batch source (`MongoDBSources.mongodb()`)  executes the 
query and emits the results as they arrive.

Following is an example pipeline which queries from `myCollection` which is in 
`myDatabase`, filters the documents which has `val` field greater than or equal
to `10`, applies the projection so that only the `val` and `_id` fields are
returned and logs them.

```java
Pipeline p = Pipeline.create();
p.drawFrom(
    MongoDBSources.mongodb(
        "batchSource", 
        "mongodb://localhost:27017",
        "myDatabase",
        "myCollection",
        new Document("val", new Document("$gte", 10)),
        new Document("val", 1).append("_id", 0)
    )
)
.drainTo(Sinks.logger());
```

### As a Stream Source

MongoDB stream source (`MongoDBSources.streamMongodb()`) watches the changes to
documents in a collection and emits these changes as they arrive. Source uses 
( `ChangeStreamDocument.getClusterTime()` ) as native timestamp.

Following is an example pipeline which watches changes on `myCollection`.
Source filters the changes so that only `insert`s which has the `val` field
greater than or equal to `10` will be fetched, applies the projection so that
only the `val` and `_id` fields are returned.

In this example we are using 

```java
Pipeline p = Pipeline.create();
p.drawFrom(
    MongoDBSources.streamMongodb(
        "streamSource",
        "mongodb://localhost:27017",
        "myDatabase",
        "myCollection",
        new Document("fullDocument.val", new Document("$gte", 10))
                        .append("operationType", "insert"),
        new Document("fullDocument.val", 1).append("_id", 1)
    )
)
.withNativeTimestamps(0)
.drainTo(Sinks.logger());
```


### As a Sink

MongoDB sink (`MongoDBSinks.mongodb()`) is used to write documents from 
Hazelcast Jet Pipeline to MongoDB. 

Following is an example pipeline which reads out items from Hazelcast
List, maps them to `Document` instances and writes them to MongoDB.

```java
Pipeline p = Pipeline.create();
p.drawFrom(Sources.list(list))
 .map(i -> new Document("key", i))
 .drainTo(
     MongoDBSinks.mongodb(
        "sink", 
        "mongodb://localhost:27017",
        "myDatabase",
        "myCollection"
     )
 );
```

Check out `com.hazelcast.jet.contrib.influxdb.MongoDBSinkTest` test class for a
more complete setup.

## Fault Tolerance

MongoDB stream source saves the resume-token of the last emitted item as a 
state to the snapshot. In case of a job restarted source will resume from the
resume-token.  

## Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
