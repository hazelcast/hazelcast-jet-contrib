# hazelcast-jet-contrib

This repository includes various community supported and incubating modules for 
[Hazelcast Jet](https://github.com/hazelcast/hazelcast-jet).

As a general guideline, the following types of modules are encouraged in this repository:

* Various connectors, including both sources and sinks
* [Context factories](https://docs.hazelcast.org/docs/jet/3.0/javadoc/com/hazelcast/jet/pipeline/ContextFactory.html).
that potentially integrate with other systems.
* Custom [aggregations](https://docs.hazelcast.org/docs/jet/3.0/javadoc/com/hazelcast/jet/aggregate/AggregateOperation.html).
These should be generic enough that they should be reusable in other pipelines.

## Building from source

To build the project, use the following command

```
./gradlew build
```

## List of modules

### [InfluxDb Connector](influxdb) 

A Hazelcast Jet Connector for InfluxDb which enables Hazelcast Jet pipelines to 
read/write data points from/to InfluxDb.

### [Probabilistic Aggregations](probabilistic) 

A collection of probabilistic aggregations such as HyperLogLog.

## Contributing

We encourage pull requests and process them promptly.

To contribute:

* see [Contribution Guideline](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/CONTRIBUTING.md)
* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* see [README Template](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/templates/README.template.md)
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

Submit your contribution as a pull request on GitHub. 

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](LICENSE) file for details
