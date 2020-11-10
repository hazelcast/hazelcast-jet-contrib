# MQTT Connector

A Hazelcast Jet connector for MQTT which enables Hazelcast Jet
pipelines to subscribe/publish to MQTT topics.

The MQTT Source is not distributed. It creates a Paho Java Client on
one of the members to connect to the MQTT broker. The client then
subscribes to the specified topic(s), and emits the payload to 
downstream.

The MQTT Sink is distributed. It creates a Paho Java Client on each
processor to connect to the MQTT broker. The client publishes messages
to the specified topic by converting each item to a `MQTTMessage`.


## Connector Attributes

### Source Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  No   |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

## Getting Started

### Installing

The MQTT Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency
to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>mqtt</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'mqtt', version: ${version}
```

## Usage

### Source

You can use `MqttSources#builder()` to build a streaming source for
MQTT topics. The source creates a Paho Java Client to connect to the
broker and subscribe to the specified topics. You can configure the
connection options using the builder.

Below is an example of a source which subscribes to a single topic
with the quality of service (QoS) `2` (`EXACTLY_ONCE`) and converts
each message to string. The source specifies that the client keeps the
session. This means the broker shall keep the messages with QoS above
`0` in case of a disconnection, and send them to the client after a
re-connect.

```java
StreamSource<String> source = MqttSources.builder()
        .topic("home/livingroom")
        .qualityOfService(EXACTLY_ONCE)
        .broker("tcp://localhost:1883")
        .keepSession()
        .mapToItemFn((topic, message) -> message.toString())
        .build();
```

User can also pass a `MqttConnectOptions` supplier function to the
builder to configure the connection options, see 
`MqttSourceBuilder#connectOptionsFn(SupplierEx<MqttConnectOptions> connectOptionsFn)`.

For more detail check out
[MqttSources](src/main/java/com/hazelcast/jet/contrib/mqtt/MqttSources.java)
and  
[MqttSourceBuilder](src/main/java/com/hazelcast/jet/contrib/mqtt/MqttSourceBuilder.java).
 
### Sink

You can use `MqttSinks#builder()` to build a sink for MQTT topics. The
sink creates a Paho Java Client for each processor. The client connects
to the broker and publishes the messages to the specified topic by
converting each item to a `MqttMessage`. You can configure the
connection options using the builder.

Below is an example of a sink which publishes the messages to a topic
with quality of service (QoS) `1` (`AT_LEAST_ONCE`). The sink enables
auto re-connect.

```java
Sink<String> sink = MqttSinks.builder()
        .topic("home/bathroom")
        .broker("tcp://localhost:1883")
        .autoReconnect()
        .<String>messageFn(item -> {
            MqttMessage message = new MqttMessage(item.getBytes());
            message.setQos(1);
            return message;
        })
        .build();
```

User can also pass a `MqttConnectOptions` supplier function to the
builder to configure the connection options, see 
`MqttSinkBuilder#connectOptionsFn(SupplierEx<MqttConnectOptions> connectOptionsFn)`.

For more detail check out
[MqttSinks](src/main/java/com/hazelcast/jet/contrib/mqtt/MqttSinks.java)
and
[MqttSinkBuilder](src/main/java/com/hazelcast/jet/contrib/mqtt/MqttSinkBuilder.java).


## Fault Tolerance

The MQTT Connector is not fault tolerant. It does not save any state to
the snapshot, but you can use `keepSession` option along with a QoS level
above `0` to signal the broker for keeping the messages when the client
disconnects. The broker sends those kept messages to the client after a
reconnection. 


## Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
