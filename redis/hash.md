# Redis Hash Connector

Redis Hashes are generic map data structures. More info can be found on the 
[Redis data types page](https://redis.io/topics/data-types). 

The Jet connectors we have here for them make it possible to read in such maps' content (in its entirety) or 
to write out (append) content to them.  

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|-------------|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  No   |
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

The Redis Hash source (`RedisSources.hash()`) connects to a specific Redis Hash and reads in all key-value pairs from it. 

Following is an example pipeline which reads in all entries from such a Hash and writes them out to a 
[Hazelcast IMap](https://docs.hazelcast.org/docs/latest/manual/html-single/#map).

```java
RedisURI uri = RedisURI.create("redis://localhost/");
Pipeline.create()
    .readFrom(RedisSources.hash("source", uri, "hash"))
    .writeTo(Sinks.map("map"));
```

For more detail check out [RedisSources](src/main/java/com/hazelcast/jet/contrib/redis/RedisSources.java) & 
[RedisSourceTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSourceTest.java).

#### As a Sink

The Redis Hash sink (`RedisSinks.hash()`) can be used to write out (append) data into a Redis Hash. 

Following is an example pipeline which reads map entries from a 
[Hazelcast IMap](https://docs.hazelcast.org/docs/latest/manual/html-single/#map) and writes them out into a Redis Hash.

```java
RedisURI uri = RedisURI.create("redis://localhost/");
Pipeline.create()
    .readFrom(Sources.map(map))
    .writeTo(RedisSinks.hash("sink", uri, "hash"));
```

For more detail check out [RedisSinks](src/main/java/com/hazelcast/jet/contrib/redis/RedisSinks.java) & 
[RedisSinksTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSinksTest.java).