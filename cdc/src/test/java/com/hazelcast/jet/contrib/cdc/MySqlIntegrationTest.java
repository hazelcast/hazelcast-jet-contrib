/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.cdc;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.cdc.data.Customer;
import com.hazelcast.jet.contrib.cdc.data.Order;
import com.hazelcast.jet.contrib.cdc.data.OrderPrimaryKey;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.contrib.cdc.Operation.DELETE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class MySqlIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    @Test
    public void customers() throws Exception {
        // given
        String[] expectedEvents = {
                "1001/0:INSERT:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                "1002/0:INSERT:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                "1003/0:INSERT:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                "1004/0:INSERT:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
        }; //todo: MySQL doesn't have SYNC operations...

        Pipeline pipeline = Pipeline.create();

        StreamSource<ChangeEvent> source = CdcSources.mysql("customers")
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setDatabaseClusterName("dbserver1")
                .setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory.customers")
                .build();

        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .<ChangeEvent>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(event -> event.key().getInteger("id").orElse(0))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, event) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            ChangeEventValue eventValue = event.value();
                            Operation operation = eventValue.operation();
                            ChangeEventElement mostRecentImage = DELETE.equals(operation) ?
                                    eventValue.before() : eventValue.after();
                            Customer customer = mostRecentImage.map(Customer.class);
                            return customerId + "/" + count + ":" + operation + ":" + customer;
                        })
                .setLocalParallelism(1)
                .writeTo(AssertionSinks.assertCollectedEventually(30, assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        //todo: would be good if we could hide the upload of connector code...
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("debezium-connector-mysql.zip")));

        // when
        JetInstance jet = createJetMember();
        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        sleepAtLeastSeconds(10);
        // update a record
        try (Connection connection = DriverManager.getConnection(mysql.withDatabaseName("inventory").getJdbcUrl(),
                mysql.getUsername(), mysql.getPassword())) {
            connection
                    .prepareStatement("UPDATE customers SET first_name='Anne Marie' WHERE id=1004")
                    .executeUpdate();
            connection
                    .prepareStatement("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')")
                    .executeUpdate();
            connection
                    .prepareStatement("DELETE FROM customers WHERE id=1005")
                    .executeUpdate();
        }

        // then
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with " +
                            "AssertionCompletedException, but completed with: " + e.getCause(),
                    errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void orders() {
        // given
        String[] expectedEvents = {
                "10001/0:INSERT:Order {orderNumber=10001, orderDate=" + new Date(1452902400000L) +
                        ", quantity=1, productId=102}",
                "10002/0:INSERT:Order {orderNumber=10002, orderDate=" + new Date(1452988800000L) +
                        ", quantity=2, productId=105}",
                "10003/0:INSERT:Order {orderNumber=10003, orderDate=" + new Date(1455840000000L) +
                        ", quantity=2, productId=106}",
                "10004/0:INSERT:Order {orderNumber=10004, orderDate=" + new Date(1456012800000L) +
                        ", quantity=1, productId=107}",
        };

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(CdcSources.mysql("orders", connectorProperties("orders")))
                .withoutTimestamps()
                .groupingKey(MySqlIntegrationTest::getOrderNumber)
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, orderId, event) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            ChangeEventValue eventValue = event.value();
                            Operation operation = eventValue.operation();
                            ChangeEventElement mostRecentImage = DELETE.equals(operation) ?
                                    eventValue.before() : eventValue.after();
                            Order order = mostRecentImage.map(Order.class);
                            return orderId + "/" + count + ":" + operation + ":" + order;
                        })
                .setLocalParallelism(1)
                .writeTo(AssertionSinks.assertCollectedEventually(30,
                        assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("debezium-connector-mysql.zip")));

        // when
        JetInstance jet = createJetMember();
        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // then
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with " +
                            "AssertionCompletedException, but completed with: " + e.getCause(),
                    errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Nonnull
    private Properties connectorProperties(String tableName) {
        Properties properties = new Properties();
        properties.put("database.hostname", mysql.getContainerIpAddress());
        properties.put("database.port", mysql.getMappedPort(MYSQL_PORT));
        properties.put("database.user", "debezium");
        properties.put("database.password", "dbz");
        properties.put("database.server.id", "184054");
        properties.put("database.server.name", "dbserver1");
        properties.put("database.whitelist", "inventory");
        properties.put("table.whitelist", "inventory." + tableName);
        return properties;
    }

    private static int getOrderNumber(ChangeEvent event) throws ParsingException {
        //pick random method for extracting ID in order to test all code paths
        boolean primitive = ThreadLocalRandom.current().nextBoolean();
        if (primitive) {
            return event.key().getInteger("order_number").orElse(0);
        } else {
            return event.key().map(OrderPrimaryKey.class).id;
        }
    }

}
