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
| Has Sink    |  No   |
| Distributed |  No   |

## Getting Started

### Installing

The HTTP(S) Listener Source artifacts published to the Maven Central repositories. 

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

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Emin Demirci](https://github.com/eminn)**
