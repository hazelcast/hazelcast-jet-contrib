# InfluxDb Sink

A simple Jet sink for InfluxDb for writing measurements.

## Getting Started

### Installing

The easiest way to start using InfluxDb Sink is to add it as a dependency to your project.

The artifacts are published on the Maven repositories. Add the following lines to your pom.xml:

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

The entry point for using the InfluxDb Sink in your pipeline is `InfluxDbSinks.influxDb()`.

Here is a very simple pipeline which reads out some measurements from Hazelcast Jet
List, maps them to the `Point` instances and writes them to the InfluxDb.

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
