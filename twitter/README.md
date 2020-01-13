# Module Title

One Paragraph of module description goes here

## Connector Attributes (Optional for non-connector extensions)

### Source Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |   No  |
| Stream      |  Yes  |
| Distributed |   No  |

### Sink Attributes
|  Atrribute  | Value |
|:-----------:|-------|
| Has Sink    |   No  |
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

<The Hazelcast Twitter Source Connector artifacts are published on the Maven repositories.>
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
## Usage
Describe the module usage and how it interacts with the rest of the system. The
entry point of the module must be included in this section like `InfluxDbSinks.influxDb()`.

End with a very small example/snippet of getting some data out of the system 
or using it for a little demo


## Running the tests

To run the tests run the command below: 

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
