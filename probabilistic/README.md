# Probabilistic Aggregations for Jet
Collections of probabilistic aggregations.
Currently implementation aggregations: HyperLogLog++

## HyperLogLog++ Aggregation for Jet 

Memory efficient cardinality estimator

### Getting Started
HyperLogLog is a probabilistic data structure to estimate cardinality. 
Translated to plain English it means this: You feed HyperLogLog with items
and it can tell you how many *unique* items it received. 

It's extremely memory efficient: The usual way to count unique elements would be
to store elements in e.g. HashSet. However this would have space complexity O(n)
where n is number of unique element. HyperLogLog has constant space complexity. 
 
What's the trade-off? Precision. There is an error rate. However for many applications
it's OK to have approximate data if it means extremely low memory consumption. 

Imagine you are processing web server access log and you want to tell how many unique
IPs visited your site. Error rate 1-2% is often irrelevant. 

You can read more about the data structure at Wikipedia: https://en.wikipedia.org/wiki/HyperLogLog   

### Usage

The aggregation is typically used in two stages:
1. Mapping step to transform your stream entries into 64 bits hashes
2. Aggregation step to estimate no. of unique hashes

In practices in looks like this:
```java
import com.hazelcast.jet.contrib.probabilistic.*;
[...]
pipeline.readFrom(Sources.mySource())
                .mapUsingContext(HashingSupport.hashingContextFactory(), HashingSupport.hashingFn()) // hash items 
                .aggregate(ProbabilisticAggregations.hyperLogLog()) // actual aggregation
                .writeTo(Sinks.mySink()); // write cardinality into sink
```


## Installing

The artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>probabilistic</artifactId>
    <version>${version}</version>
</dependency>
```


Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'probabilistic', version: ${version}
```



## Running the tests

To run the tests run the command below: 

```
./gradlew test
```
## Authors

* **[Jaromir Hamala](https://github.com/jerrinot)**
## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details