# Hazelcast Jet Spring Boot Starter

Hazelcast Jet Spring Boot Starter which enables Hazelcast Jet to be 
auto-configured in Spring Boot environment.

## Getting Started

### Installing

Hazelcast Jet Spring Boot Starter artifacts are published on the Maven
repositories. 

Add the following lines to your pom.xml to include it as a dependency
to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>hazelcast-jet-spring-boot-starter</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'hazelcast-jet-spring-boot-starter', version: ${version}
```

### Usage

If [Hazelcast Jet](https://jet.hazelcast.org/) is on the classpath and
a suitable configuration is found, the starter auto-configures a
`JetInstance` that you can inject in your application.

#### Config Beans

If you define a `JetConfig` bean, the starter creates a Jet server
using that bean or if you define a `ClientConfig` bean, the starter
creates a Jet client using that bean.

#### System Properties

You can define configuration files using system properties. For
Hazelcast Jet server and Hazelcast IMDG:

```java
System.setProperty("hazelcast.jet.config", "config/hazelcast-jet-specific.yaml");
System.setProperty("hazelcast.config", "config/hazelcast-specific.yaml");
``` 

For Hazelcast Jet client:

```java
System.setProperty("hazelcast.client.config", "config/hazelcast-client-specific.yaml");
```

#### Config Properties

You can define configuration files using config properties. For
Hazelcast Jet server and Hazelcast IMDG:

```properties
hazelcast.jet.server.config=classpath:config/hazelcast-jet-specific.yaml
hazelcast.jet.imdg.config=classpath:config/hazelcast-specific.yaml
```

For Hazelcast Jet client:

```properties
hazelcast.jet.client.config=classpath:config/hazelcast-client-specific.yaml
```

#### Default Configuration Files

If no configuration file is defined explicitly, Spring Boot tries to
find default configuration files in the working directory or classpath. 

See the 
[Hazelcast Jet documentation](https://docs.hazelcast.org/docs/jet/latest/manual/#declarative-configuration)
 for more details.

Spring Boot attempts to create a `JetInstance` by checking following 
configuration options:

* The presence of a `JetConfig` bean (a Jet member will be created).
* The presence of the `hazelcast.jet.config` system property (a Jet
member will be created).
* A configuration file defined by the 
`configprop:hazelcast.jet.server.config[]` property (a Jet member will
 be created).
* The presence of a `ClientConfig` bean (a Jet client will be created).
* The presence of the `hazelcast.client.config` system property (a Jet
client will be created).
* A configuration file defined by the 
`configprop:hazelcast.jet.client.config[]` property (a Jet
client will be created).
* `hazelcast-jet.yaml`, `hazelcast-jet.yml` or `hazelcast-jet.xml` in
the working directory or at the root of the classpath. (a Jet member
will be created).
* `hazelcast-client.yaml`, `hazelcast-client.yml` or 
`hazelcast-client.xml` in the working directory or at the root of the 
classpath. (a Jet client will be created).
* If none of the above conditions are matched, a Jet member will be 
created using the default configuration file from Hazelcast Jet 
distribution (`hazelcast-jet-default.yaml`). 

Hazelcast Jet Spring Boot Starter is compatible with Spring Boot 2.0.0
and above.

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
