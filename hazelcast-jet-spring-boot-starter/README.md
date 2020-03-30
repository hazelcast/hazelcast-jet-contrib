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
    <artifactId>jet-spring-boot-starter</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'jet-spring-boot-starter', version: ${version}
```

### Usage

If [Hazelcast Jet](https://jet.hazelcast.org/) is on the classpath and
a suitable configuration is found, Spring Boot auto-configures a
`JetInstance` that you can inject in your application.

If you define a `com.hazelcast.jet.config.JetConfig` bean, Spring Boot
uses that.

You could also specify the Hazelcast Jet server configuration file to
use through configuration, as shown in the following example:

```text
spring.hazelcast.jet.config=classpath:config/my-hazelcast-jet.xml
```

Or you could specify Hazelcast Jet client configuration as shown in the
following example:

```text
spring.hazelcast.jet.client.config=classpath:config/my-hazelcast-client.xml
```

Otherwise, Spring Boot tries to find the Hazelcast Jet configuration
from the default locations: `hazelcast-jet.xml` in the working
directory or at the root of the classpath, or a `.yaml`/`.yml`
counterpart in the same locations.

We also check if the `hazelcast.jet.config` system property set for
server configuration and `hazelcast.client.config` system property set
for client configuration.

See the 
[Hazelcast Jet documentation](https://docs.hazelcast.org/docs/jet/latest/manual/#declarative-configuration)
 for more details.

Spring Boot attempts to create a `JetInstance` by checking following 
configuration options:

* The presence of a `com.hazelcast.jet.config.JetConfig` bean (a Jet
member will be created).
* The presence of the `hazelcast.jet.config` system property (a Jet
member will be created).
* A configuration file defined by the 
`configprop:spring.hazelcast.jet.config[]` property (a Jet member will
 be created).
* The presence of a `com.hazelcast.client.config.ClientConfig` bean (a
Jet client will be created).
* The presence of the `hazelcast.client.config` system property (a Jet
client will be created).
* A configuration file defined by the 
`configprop:spring.hazelcast.jet.client.config[]` property (a Jet
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
