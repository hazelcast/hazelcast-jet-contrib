# hazelcast-jet-contrib

This repository includes various community supported and incubating 
modules for [Hazelcast Jet](https://github.com/hazelcast/hazelcast-jet).

As a general guideline, the following types of modules are encouraged
in this repository:

* Various connectors, including both sources and sinks
* [Context factories](https://docs.hazelcast.org/docs/jet/3.0/javadoc/com/hazelcast/jet/pipeline/ContextFactory.html).
that potentially integrate with other systems.
* Custom [aggregations](https://docs.hazelcast.org/docs/jet/3.0/javadoc/com/hazelcast/jet/aggregate/AggregateOperation.html).
These should be generic enough that they should be reusable in other
 pipelines.

## Building from source

To build the project, use the following command

```
./gradlew build
```

## List of modules

### [InfluxDb Connector](influxdb) 

A Hazelcast Jet Connector for InfluxDb which enables Hazelcast Jet
pipelines to read/write data points from/to InfluxDb.

### [Probabilistic Aggregations](probabilistic) 

A collection of probabilistic aggregations such as HyperLogLog.

### [Elasticsearch Connector](elasticsearch) 

A Hazelcast Jet connector for Elasticsearch for querying/indexing objects
from/to Elasticsearch.

### [Redis Connectors](redis)

Hazelcast Jet connectors for various Redis data structures.

### [MongoDB Connector](mongodb) 

A Hazelcast Jet connector for MongoDB for querying/inserting objects
from/to MongoDB.

### [Kafka Connect Connector](kafka-connect) 

A generic Kafka Connect source provides ability to plug any Kafka
Connect source for data ingestion to Jet pipelines.

### [Debezium Connector](debezium) 

A Hazelcast Jet connector for [Debezium](https://debezium.io/) which
enables Hazelcast Jet pipelines to consume CDC events from various databases.


### [Twitter Connector](twitter) 

A Hazelcast Jet connector for consuming data from Twitter stream 
sources in Jet pipelines.

### [Pulsar Connector](pulsar) 

A Hazelcast Jet connector for consuming/producing messages from/to Apache Pulsar topics.

### [XA Tests](xa-test)

Tests to check compatibility of the XA support in your JMS broker or
JDBC database with Jet's fault tolerance.

### [Hazelcast Jet Spring Boot Starter](hazelcast-jet-spring-boot-starter)

A Spring Boot Starter for Hazelcast Jet which auto-configures Hazelcast
Jet if found on the classpath.

### [HTTP(S) Listener Source](http) 

A Hazelcast Jet source for listening HTTP(S) requests which contains JSON payload.


## Snapshot Releases

To access snapshot builds add the following `dependency` and 
`repository` declarations to `dependencies` and `repositories` sections
in your `pom.xml` respectively.
              
```xml
<dependency>
  <groupId>com.hazelcast.jet.contrib</groupId>
  <artifactId>${module.name}</artifactId>
  <version>${module.version}</version>
</dependency>
```

```xml
<repository>
    <id>sonatype-snapshots</id>
    <name>Sonatype Snapshot Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
````

## Contributing

We encourage pull requests and process them promptly.

To contribute:

* see [Contribution Guideline](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/CONTRIBUTING.md)
* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* see [README Template](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/templates/README.template.md)
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

Submit your contribution as a pull request on GitHub. 

## License

This project is licensed under the Apache 2.0 license - see the
[LICENSE](LICENSE) file for details
