# Stream deduplication for Hazelcast Jet

Remove duplicates items from a stream.

Unlike [StageWithWindow#distinct()](https://github.com/hazelcast/hazelcast-jet/blob/0407c909f6107d1be42ca8e4cf420e44aa8fcadc/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/StageWithWindow.java#L88)
or [StageWithKeyAndWindow#distinct()](https://github.com/hazelcast/hazelcast-jet/blob/0407c909f6107d1be42ca8e4cf420e44aa8fcadc/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/StageWithKeyAndWindow.java#L71)
it emits distinct elements as soon as they arrive without waiting for a window to close. 
Thus it won't introduce latency to a stream processing pipeline.

It creates a window per each distinct element. Each window has a configurable length.

When a first element arrives it is immediately emitted to downstream stages and a new
window is created. When an equal element arrives later and its matching window is still
open then this element is discarded and validity of the window is extended.

Deduplication window is driven by event-time hence the window length is typically in milliseconds.

### Getting Started
See this test:
```java
import static com.hazelcast.jet.contrib.deduplication.Deduplications.deduplicationWindow;
[...]

@Test
public void testDeduplication() {
    Pipeline pipeline = Pipeline.create();

    pipeline.drawFrom(mySource())
        .withNativeTimestamps(ALLOWED_LAG)
        .apply(deduplicationWindow(WINDOW_LENGTH)) // <-- add the deduplication stage
        .drainTo(Sinks.logger());

    JetInstance instance1 = Jet.newJetInstance();
    JetInstance instance2 = Jet.newJetInstance();
    Job job = instance1.newJob(pipeline);

    assertNoDuplicates(job);
}
```

### Sizing considerations
Memory consumption grows with no. of district elements. Each distinct
elements is held in memory while its matching window is still valid.

Thus having too large window increase memory consumption. Having too
small window might lead to duplicates not being detected. 


## Installing

The artifacts are published on the Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>deduplication</artifactId>
    <version>${version}</version>
</dependency>
```


Gradle:
```
compile group: 'com.hazelcast.jet.contrib', name: 'deduplication', version: ${version}
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
file for details.
