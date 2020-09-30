# HTTP(S) Listener Connector

A Hazelcast Jet source for listening HTTP(S) requests, and a sink which
clients can connect and listen the items using either websocket or
server-sent events.

The source creates an [Undertow](https://github.com/undertow-io/undertow)
server for each processor and starts listening the incoming requests.
User can configure the host and port for the server. User can also
define a ssl context to secure the connections. The payload can be
converted to the desired output before emitting to the downstream.

The sink is not distributed. Sink creates a single [Undertow](https://github.com/undertow-io/undertow)
server on one of the servers randomly chosen, and starts listening for
the clients to connect. Clients can connect using either websocket or
server-sent events. The sink can accumulate items if there is no
connected client.

## Connector Attributes

### Source Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  No   |
| Stream      |  Yes  |
| Distributed |  Yes  |

### Sink Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  No   |

## Getting Started

### Installing

The http module artifacts published to the Maven Central repositories. 

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>http</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'http', version: ${version}
```

### Usage

#### HTTP Listener Source

HTTP Listener Source creates HTTP listener on each Hazelcast Jet member
on user configured host and port.

Below is an example pipeline which the source maps payload JSON
messages to POJOs of provided class, filters them based on its age
property and logs them to standard output.

```java
Pipeline p = Pipeline.create();
p.readFrom(HttpListenerSources.httpListener(8080, Employee.class))
 .withoutTimestamps()
 .filter(employee -> employee.getAge() < 25)
 .writeTo(Sinks.logger());
```


#### Http Listener Sink for Websocket

Http Listener Sink for Websocket creates a listener for websocket
connections on one of the Hazelcast Jet members. The sink converts each
item to string using the provided `toStringFn` and sends to connected
websocket clients. The sink uses `Object#toString` by default.   


Below is an example pipeline which generates 5 items per second and
publishes those items with the websocket server. After the job has been
submitted, you can use `HttpListenerSinks.sinkAddress(JetInstance, Job)`
static method to retrieve the server address. You can use that address
with any websocket client to start streaming the results.

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(5))
        .withoutTimestamps()
        .writeTo(HttpListenerSinks.websocket("/items", 8080));

Job job = jet.newJob(p);
String sinkAddress = HttpListenerSinks.sinkAddress(jet, job);
//sinkAddress: "ws://the-host:8080/items
```

Check out the [HttpListenerSinkTest](./src/test/java/com/hazelcast/jet/contrib/http/HttpListenerSinkTest.java)
for an example implementation with Undertow WebSocket Client.

#### Http Listener Sink for Server-Sent-Events

Http Listener Sink for Server-Sent Events creates a listener for
http connections on one of the Hazelcast Jet members. The sink converts
each item to string using the provided `toStringFn` and sends to
connected http clients. The sink uses `Object#toString` by default.

Below is an example pipeline which generates 5 items per second and
publishes those items with the http server using SSE. After the job has
been submitted, you can use `HttpListenerSinks.sinkAddress(JetInstance, Job)`
static method to retrieve the server address. You can use that address
with any http client which has SSE support to start streaming the
results.

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(5))
        .withoutTimestamps()
        .writeTo(HttpListenerSinks.sse("/items", 8080));

Job job = jet.newJob(p);
String sinkAddress = HttpListenerSinks.sinkAddress(jet, job);
//sinkAddress: "http://the-host:8080/items
```

While the pipeline above runs, if you make a GET request to the HTTP endpoint 
you should see a similar output like below:

```bash
$ curl -X GET http://192.168.1.25:8080/items  
data:SimpleEvent(timestamp=21:20:21.000, sequence=41)

data:SimpleEvent(timestamp=21:20:21.200, sequence=42)

data:SimpleEvent(timestamp=21:20:21.400, sequence=43)

data:SimpleEvent(timestamp=21:20:21.600, sequence=44)

data:SimpleEvent(timestamp=21:20:21.800, sequence=45)

data:SimpleEvent(timestamp=21:20:22.000, sequence=46)

data:SimpleEvent(timestamp=21:20:22.200, sequence=47)

data:SimpleEvent(timestamp=21:20:22.400, sequence=48)

```


### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
* **[Ali Gurbuz](https://github.com/gurbuzali)**
