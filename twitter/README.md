# Twitter Source

A Hazelcast Jet connector for Twitter Streaming API which 
enables Hazelcast Jet pipelines to consume tweets from 
multiple Twitter APIs:
- Twitter’s Streaming API – Push from Twitter, real-time stream,
 it samples tweets (1-40% of Tweets can be consumed this way)
 
- Twitter’s Search API – result limited to 15 tweets
 per request, and 180 requests every 15 minutes.



## Connector Attributes

### Source Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  Yes  |
| Distributed |   No  |

### Sink Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Sink    |   No  |
| Distributed |   No  |


## Getting Started

### Installing

The Hazelcast Twitter Source Connector artifacts are published on the Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>twitter</artifactId>
    <version>${version}</version>
</dependency>
```
If you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'twitter', version: ${version}
```
## Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **Ufuk Yilmaz**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details
