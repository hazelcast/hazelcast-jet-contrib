# InfluxDb Connector

A Hazelcast Jet connector for InfluxDb which enables Hazelcast Jet pipelines to 
read/write data points from/to InfluxDb.

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  No   |
| Distributed |  No   |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

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

InfluxDb batch source (`InfluxDbSources.influxDb()`)  executes the 
query and emits the results as they arrive.

Following is an example pipeline which queries from InfluxDb, maps the first and
second column values on the row to a tuple and logs them.

```java
Pipeline p = Pipeline.create();
p.readFrom(
        InfluxDbSources.influxDb("SELECT * FROM db..cpu_usages",
                DATABASE_NAME,
                INFLUXDB_URL,
                USERNAME,
                PASSWORD,
                (name, tags, columns, row) -> tuple2(row.get(0), row.get(1))))
)
 .writeTo(Sinks.logger());
```

Check out [InfluxDbSourceTest](src/test/java/com/hazelcast/jet/contrib/influxdb/InfluxDbSourceTest.java) test class 
for a more complete setup.


#### As a Sink

InfluxDb sink (`InfluxDbSinks.influxDb()`) is used to write data points from 
Hazelcast Jet Pipeline to InfluxDb . 

Following is an example pipeline which reads out measurements from Hazelcast
List, maps them to `Point` instances and writes them to InfluxDb.

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.list(measurements))
 .map(index -> Point.measurement("mem_usage")
                    .time(System.nanoTime(), TimeUnit.NANOSECONDS)
                    .addField("value", index)
                    .build())
 .writeTo(InfluxDbSinks.influxDb(DB_URL, DATABASE_NAME, USERNAME, PASSWORD));
```

Check out [InfluxDbSinkTest](src/test/java/com/hazelcast/jet/contrib/influxdb/InfluxDbSinkTest.java) test class for a 
more complete setup.

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Can Gencer](https://github.com/cangencer)**
* **[Emin Demirci](https://github.com/eminn)**
