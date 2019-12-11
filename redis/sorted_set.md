# Redis Sorted Set Connector

Redis Sorted Sets are non repeating collections of elements, where each element is associated with a numeric score.
The score is used to provide ordering in the set, from smallest score to largest. More info can be found on the 
[Redis data types page](https://redis.io/topics/data-types). 

Sorted Sets are quite often used for storing time-series data by using timestamps as score values. In this 
situation the most frequent use case is interrogating the sorted set for a range of timestamped values, in essence 
all values having their scores in a certain interval. This is the case this Jet source addresses.

The Sorted Set sink on the other hand makes it possible to write out (append) scored values into such Sorted Sets.  

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

The Redis Sorted Set source (`RedisSources.sortedSet()`) connects to a specific Redis Sorted Set and retrieves from it a 
range of entries, those having their scores between a specified min and max value.  

Following is an example pipeline which reads such a range from a Sorted Set, maps the items to strings and 
drains them to some sink.

```java
RedisURI uri = RedisURI.create("redis://localhost/");
Pipeline.create()
    .readFrom(RedisSources.sortedSet("source", uri, "sortedSet", 10d, 90d))
    .map(sv -> (int) sv.getScore() + ":" + sv.getValue())
    .writeTo(sink);
```

For more detail check out [RedisSources](src/main/java/com/hazelcast/jet/contrib/redis/RedisSources.java) & 
[RedisSourceTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSourceTest.java).

#### As a Sink

The Redis Sorted Set sink (`RedisSinks.sortedSet()`) is used to write data points from a Jet Pipeline to a 
Redis Sorted Set. 

Following is an example pipeline which reads out trades from a source and writes them to a Redis Stream.

```java
RedisURI uri = RedisURI.create("redis://localhost/");
Pipeline.create()
    .readFrom(source)
    .map(trade -> ScoredValue.fromNullable(trade.timestamp, trade))
    .writeTo(RedisSinks.sortedSet("sink", uri, "sortedSet"));
```

For more detail check out [RedisSinks](src/main/java/com/hazelcast/jet/contrib/redis/RedisSinks.java) & 
[RedisSinksTest](src/test/java/com/hazelcast/jet/contrib/redis/RedisSinksTest.java).