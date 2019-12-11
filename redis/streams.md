# Redis Streams Connector

Redis Streams are an in-memory data structure of Redis. They are in-memory logs, a.k.a. in-memory Kafka. You can read 
more about them [here](https://redis.io/topics/streams-intro).

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|-------------|-------|
| Has Source  |  Yes  |
| Batch       |  No   |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Atrribute  | Value |
|-------------|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

## Getting Started

### Installing

For installation and other prerequisites see the [main page](README.md#prerequisites-for-all-connectors).

### Usage

#### As a Source

The Redis Streams source (`RedisSources.stream()`) connects to one or more Redis Streams, requests data from a 
starting offset for each of them and emits the values as they arrive.

Following is an example pipeline which reads all elements from two different Redis Streams, maps the objects received
to a stream specific ID (mapping feature built into the source) and drains the results out to some generic sink.

```java
Map<String, String> streamOffsets = new HashMap<>();
streamOffsets.put("streamA", "0");
streamOffsets.put("streamB", "0");

RedisURI uri = RedisURI.create("redis://localhost/");

Pipeline.create()
    .readFrom(RedisSources.stream("source", uri, streamOffsets,
                    mes -> mes.getStream() + " - " + mes.getId()))
    .withoutTimestamps()
    .writeTo(sink);
```

For more detail check out [RedisSources](src/main/java/com/hazelcast/jet/contrib/redis/RedisSources.java) & 
[RedisSourceTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSourceTest.java).

#### As a Sink

The Redis Streams sink (`RedisSinks.stream()`) is used to write data points from a Jet Pipeline to Redis Streams . 

Following is an example pipeline which reads out measurements from Hazelcast List and writes them to a Redis Stream.

```java
RedisURI uri = RedisURI.create("redis://localhost/");
Pipeline.create()
    .readFrom(Sources.list(list))
    .writeTo(RedisSinks.stream("sink", uri, "stream"));
```

For more detail check out [RedisSinks](src/main/java/com/hazelcast/jet/contrib/redis/RedisSinks.java) & 
[RedisSinksTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSinksTest.java).
