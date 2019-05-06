# InfluxDb Connector

A Hazelcast Jet connector for InfluxDb which enables Hazelcast Jet pipelines to 
read/write data points from/to InfluxDb.

## Getting Started

### Installing

The InfluxDb Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>influxdb</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'influxdb', version: ${version}
```

### Usage

#### As a Source

There are two APIs for querying data to be consumed by Hazelcast Jet from InfluxDb.


##### Batch Source

InfluxDb batch source (`InfluxDbSources.influxDb()`)  executes the 
query and retrieves all the results in one go.

Following is an example pipeline which queries from InfluxDb and logs the results.

```java
Pipeline p = Pipeline.create();
p.drawFrom(
        InfluxDbSources.influxDb("SELECT * FROM db..cpu_usages",
                DATABASE_NAME,
                INFLUXDB_URL,
                USERNAME,
                PASSWORD)
)
 .flatMap(series -> Traversers.traverseIterable(series.getValues()))
 .drainTo(Sinks.logger());
```

##### Streaming Source
InfluxDb streaming source (`InfluxDbSources.streamInfluxDb()`) executes
a streaming query and emits the results as they arrive.
   
Following is an example pipeline which streams the query results from InfluxDb 
and logs the results.

```java
Pipeline p = Pipeline.create();
p.drawFrom(
        InfluxDbSources.streamInfluxDb("SELECT * FROM db..cpu_usages",
                DATABASE_NAME,
                INFLUXDB_URL,
                USERNAME,
                PASSWORD,
                CHUNK_SIZE)
)
 .withoutTimestamps()
 .flatMap(series -> Traversers.traverseIterable(series.getValues()))
 .drainTo(Sinks.logger());
```

#### As a Sink

InfluxDb sink (`InfluxDbSinks.influxDb()`) is used to write data points from 
Hazelcast Jet Pipeline to InfluxDb . 

Following is an example pipeline which reads out measurements from Hazelcast
List, maps them to `Point` instances and writes them to InfluxDb.

```java
Pipeline p = Pipeline.create();
p.drawFrom(Sources.list(measurements))
 .map(index -> Point.measurement("mem_usage")
                    .time(System.nanoTime(), TimeUnit.NANOSECONDS)
                    .addField("value", index)
                    .build())
 .drainTo(InfluxDbSinks.influxDb(DB_URL, DATABASE_NAME, USERNAME, PASSWORD));
```

Check out `com.hazelcast.jet.influxdb.InfluxDbSinkTest` test class for a more 
complete setup.

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Can Gencer](https://github.com/cangencer)**
* **[Emin Demirci](https://github.com/eminn)**
