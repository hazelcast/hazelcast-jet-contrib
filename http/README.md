# HTTP(S) Listener Source

A Hazelcast Jet source for listening HTTP(S) requests which contains JSON payload.

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
| Has Sink    |  Yes  |
| Distributed |  Yes  |

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

#### HTTP Source

HTTP Listener Source (`HttpListenerSource.httpListener()`) creates HTTP listener on 
the same host with the Hazelcast Jet instance on user configured port offset. 

Imagine if we have a running Hazelcast Jet member running on `localhost:5701`,
if we submit following pipeline which listens for HTTP messages with port offset `100`,
the actual port of the HTTP listener will be `5801` (base port(`5701`) + port offset(`100`)).
The source will map payload JSON messages to POJOs of provided class, filters them based 
on its age property and logs them to standard output.

```java
Pipeline p = Pipeline.create();
p.readFrom(HttpListenerSources.httpListener(100, Employee.class))
 .withoutTimestamps()
 .filter(employee -> employee.getAge() < 25)
 .writeTo(Sinks.logger());
```

#### HTTPS Source

HTTPS Listener Source (`HttpListenerSource.httpsListener()`) creates HTTP listener on 
the same host with the Hazelcast Jet instance on user configured port offset. 

Imagine if we have a running Hazelcast Jet member running on `localhost:5701`,
if we submit following pipeline which listens for HTTP messages with port offset `100`,
the actual port of the HTTP listener will be `5801` (base port(`5701`) + port offset(`100`)).
The source will map payload JSON messages to POJOs of provided class, filters them based 
on its age property and logs them to standard output.

```java
SupplierEx<SSLContext> contextSupplier = () -> {
    // Initialize the SSLContext from key store and trust stores
    // This uses mock key and trust stores, replace them with your key
    // and trust store files along with their passwords.
    SSLContext context = SSLContext.getInstance("TLS");
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    KeyStore ks = KeyStore.getInstance("JKS");
    char[] password = "123456".toCharArray();
    File ksFile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.keyStore);
    ks.load(new FileInputStream(ksFile), password);
    kmf.init(ks, password);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    KeyStore ts = KeyStore.getInstance("JKS");
    File tsFile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.trustStore);
    ts.load(new FileInputStream(tsFile), password);
    tmf.init(ts);

    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    return context;
};

Pipeline p = Pipeline.create();
p.readFrom(HttpListenerSources.httpsListener(portOffset, contextSupplier, Employee.class))
 .withoutTimestamps()
 .filter(employee -> employee.getAge() < 25)
 .writeTo(Sinks.logger());
```

#### Websocket Server Sink

Below is an example pipeline which generates 5 items per second and publishes those items
with the websocket server. After the job has been submitted, you can use `HttpSinks.getWebSocketAddress()`
static method to retrieve the websocket server address. You can use that address with any websocket client
to start streaming the results.

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(5))
        .withoutTimestamps()
        .writeTo(HttpSinks.websocket("/items", 100));

Job job = jet.newJob(p);
String webSocketAddress = HttpSinks.getWebSocketAddress(jet, job);
```

Check out the `HttpSinkTest` for an example implementation with Undertow WebSocket Client.

#### Server-Sent Events Server Sink

Below is an example pipeline which generates 5 items per second and publishes those items
with the http server using SSE. After the job has been submitted, you can use `HttpSinks.getSseAddress()`
static method to retrieve the server address. You can use that address with any http client which has
sse support to start streaming the results.

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(5))
        .withoutTimestamps()
        .writeTo(HttpSinks.sse("/items", 100));

Job job = jet.newJob(p);
String sseAddress = HttpSinks.getSseAddress(jet, job);
```

While the pipeline above runs, if you make a GET request to the HTTP endpoint 
you should see a similar output like below:

```bash
$ curl -X GET http://192.168.1.25:5801/items  
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
