# Redis Connectors

Hazelcast Jet connector for various [Redis](https://redis.io/) data structures which enables Hazelcast Jet pipelines to 
read/write data points from/to them:

- [Streams](streams.md)
- [Sorted Set](sorted_set.md)
- [Hash](hash.md)

## Prerequisites for all connectors

### Redis 

First thing you will need is a Redis installation. Check out the [Redis Quick Start](https://redis.io/topics/quickstart)
guide on how to do this. 

For development, the fastest way to get it working is to use [Homebrew](https://brew.sh/):
- `$ brew install redis` installs it
- `$ brew info redis` can check the installed version
- `$ brew services start redis` starts it as a service
- `$ redis-cli ping` can be used to make sure it's running 
- `$ brew services stop redis` stops the service
- `$ brew uninstall redis` uninstalls it and its files 

### Java client

Redis Connector artifacts are published on the Maven repositories. The connectors in turn also have a dependency of
their own on the [Lettuce](https://github.com/lettuce-io/lettuce-core) Redis Java client, so that jar must also be
on the classpath. 

Add the following lines to your pom.xml:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>redis</artifactId>
    <version>${version}</version>
</dependency>

<dependency>
    <groupId>io.lettuce</groupId>
    <artifactId>lettuce-core</artifactId>
    <version>5.1.7.RELEASE</version>
</dependency>
```

Or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'redis', version: ${version}
compile group: 'io.lettuce', name: 'lettuce-core', version: '5.1.7.RELEASE'
```

## Running the tests

To run the tests run the command below: 

```
../gradlew test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
* **[Jozsef Bartok](https://github.com/jbartok)**