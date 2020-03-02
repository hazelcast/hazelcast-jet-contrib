# XA Tests

A set of tests to check compatibility of the XA support in your JMS
broker or JDBC database with Jet's fault tolerance.

## Usage

Before running, you need to add dependency to your JDBC driver or JMS
broker to [build.gradle] and you need to edit the source code to create
and configure your XA connection factory. You can check out this
repository or temporarily copy-paste into any project in your IDE.

The package contains two tests:

* [JDBC test](src/main/java/JdbcXaTest.java)
* [JMS test](src/main/java/JmsXaTest.java)

Follow the comments in the code.

## Authors

* **Viliam Ďurina**

See also the list of [contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors)
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the [LICENSE](../LICENSE)
file for details
