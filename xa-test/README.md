# XA Tests

A set of tests to check compatibility of the XA support in your JMS
broker or JDBC database with Jet's fault tolerance.

## Usage

This package is not intended to be used as a dependency. You need to
checkout the code, add a dependency to your JDBC driver or JMS broker to
[build.gradle] and edit the source code to create and configure your XA
connection factory. Alternatively, you can copy-paste the code to your
project.

The package contains two tests:

* [JDBC test](src/main/java/com/hazelcast/jet/contrib/xatests/JdbcXaTest.java)
* [JMS test](src/main/java/com/hazelcast/jet/contrib/xatests/JmsXaTest.java)

The tests check only one feature, namely that a prepared transaction can
be committed after the client reconnects. Follow the comments in the
code.

## Known results

The following brokers were tested by the community:

|Provider|Result|
|---|---|
|**JDBC**||
|PostgreSQL 12.1|OK|
|MySQL 8.0|OK|
|H2 Database 1.4|Broken|
|HSQLDB 2.4|Broken|
|MariaDB 10.5|Broken|
|**JMS**||
|ActiveMQ 5.15|OK|
|ActiveMQ Artemis 2.11|OK|
|RabbitMQ 3.8|no XA support|


If you test with another JMS/JDBC provider, a pull request with an
update to this table is welcome.

## Authors

* **Viliam Ďurina**

See also the list of
[contributors](https://github.com/hazelcast/hazelcast-jet-contrib/graphs/contributors)
who participated in this project.

## License

This project is licensed under the Apache 2.0 license - see the
[LICENSE](../LICENSE) file for details
