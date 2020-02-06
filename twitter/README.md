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

## Usage

### Reading from Twitter's Streaming API
To use Twitter Streaming Endpoints as a source in your pipeline you need
to create a source by calling the `TwitterSources.stream() or
TwitterSources.timestampedStream()` methods with a `Properties` object
that includes Twitter credentials, and a supplier function(`SupplierEx`)
that provides a specific Twitter streaming endpoint.

The Twitter Stream Source provides tweets as JSON formatted raw strings.
You can extract related fields from tweets by parsing them into a JSON
objects.

Here's an example job which the Jet pipelines ingest tweets from
Twitter's Filtered Stream Endpoint that filters the tweets by keywords.
```java
Properties credentials = new Properties();
properties.setProperty("consumerKey", "???"); // OAuth1 Consumer Key
properties.setProperty("consumerSecret", "???"); // OAuth1 Consumer Secret
properties.setProperty("token", "???"); // OAuth1 Token
properties.setProperty("tokenSecret", "???"); // OAuth1 Token Secret
List<String> terms = new ArrayList<>(Arrays.asList("BTC", "ETH"));
StreamSource<String> streamSource =
             TwitterSources.stream(
                     credentials,
                     () -> new StatusesFilterEndpoint().trackTerms(terms)
             );
Pipeline p = Pipeline.create();
p.readFrom(streamSource)
        .withoutTimestamps()
        .writeTo(Sinks.logger());
JetInstance jet = createJetMember();
Job job = jet.newJob(p);
job.join();
```

For the demo application using Twitter Stream Source see:
[Cryptocurrency Sentiment Analysis](https://github.com/hazelcast/hazelcast-jet-demos/tree/master/cryptocurrency-sentiment-analysis/)

### Getting tweets From Twitter's Search API

To use Twitter Search Endpoint as a source in your pipeline you need to
create a source by calling the `TwitterSources.search()` method with a
`Properties` object that includes Twitter credentials, and a `String`
that includes search query. Twitter search source (`TwitterSources.search()`)
executes the query and emits the results as they arrive.

Because Twitter uses the pagination technique at this endpoint, Twitter Search
Connector performs multiple requests for the same query to get all available
pages. Twitter has a rate limit for the search endpoint that limits the number
of these search requests to 180 calls every 15 minutes. Twitter Connector
repeatedly makes search requests until it gets all available pages or the
rate-limit is exceeded.

The Search Source provides tweets as Status object that enables the user extract
related fields of it by using its getter methods.

Here is an example job which ingests tweets that are related to the `Jet flies`
search query.

```java
Properties credentials = new Properties();
properties.setProperty("consumerKey", "???"); // OAuth1 Consumer Key
properties.setProperty("consumerSecret", "???"); // OAuth1 Consumer Secret
properties.setProperty("token", "???"); // OAuth1 Token
properties.setProperty("tokenSecret", "???"); // OAuth1 Token Secret
String query = "Jet flies";
BatchSource<Status> searchSource = TwitterSources.search(credentials, query);
Pipeline p = Pipeline.create();
p.readFrom(searchSource)
        .map(status -> "@" + status.getUser().getName() + " - " + status.getText())
        .writeTo(Sinks.logger);
JetInstance jet = createJetMember();
Job job = jet.newJob(p);
job.join(); 
```

For more detail check out:
[TwitterSources](src/main/java/com/hazelcast/jet/contrib/twitter/TwitterSources.java),
[TwitterSourceTest](src/test/java/com/hazelcast/jet/contrib/twitter/TwitterSourceTest.java).


## Authors

* **Ufuk Yilmaz**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details
