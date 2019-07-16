# HTTP Listener Source

A Hazelcast Jet source for listening HTTP requests which contains JSON payload.

## Connector Attributes

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  No   |
| Stream      |  Yes  |
| Distributed |  Yes  |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |  No   |
| Distributed |  No   |

## Getting Started

### Installing

The HTTP Listener Source artifacts are published on the Maven repositories. 

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

#### As a Source

HTTP Listener Source (`HttpServerSource.httpServer()`) creates HTTP server on 
the same host with the Hazelcast Jet instance on user configured port offset. 

Imagine if we have a running Hazelcast Jet member running on `localhost:5701`,
if we submit following pipeline which listens for HTTP messages with port offset `100`,
the actual port of the HTTP server will be `5801` (base port(`5701`) + port offset(`100`)).
Then it maps the JSON messages to JSON objects, filters them based 
on its id property and logs them to standard output.

```java
Pipeline p = Pipeline.create();
p.drawFrom(HttpServerSource.httpsServer(100))
 .withoutTimestamps()
 .map(JsonValue::asObject)
 .filter(object -> object.get("id").asInt() >= 80)
 .drainTo(Sinks.logger());
```



### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
