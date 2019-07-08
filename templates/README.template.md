# Module Title

One Paragraph of module description goes here

## Connector Attributes (Optional for non-connector extensions)

|  Atrribute  | Value |
|:-----------:|-------|
| Bounded     |   No  |
| Unbounded   |  Yes  |
| Source      |  Yes  |
| Sink        |  Yes  |
| Replayable  |  Yes  |
| Distributed |   No  |


## Getting Started

These instructions will get you a copy of the project up and running on your local
 machine for development and testing purposes. 

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you how to get a development env running

This is a good place to put Maven and Gradle dependencies.

Maven:
```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>influxdb</artifactId>
    <version>${version}</version>
</dependency>
```

Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'influxdb', version: ${version}
```

## Usage

Describe the module usage and how it interacts with the rest of the system. The
entry point of the module must be included in this section like `InfluxDbSinks.influxDb()`.

End with a very small example/snippet of getting some data out of the system 
or using it for a little demo


## Running the tests

Explain how to run the automated tests for this system

```
./gradlew test
```

## Authors

* **Name Surname** - link to your profile or contact information if you like

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors) 
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) 
file for details
