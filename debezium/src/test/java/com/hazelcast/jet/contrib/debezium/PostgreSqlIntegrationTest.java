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

package com.hazelcast.jet.contrib.debezium;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.Parser;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import io.debezium.config.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgreSqlIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("debezium/example-postgres:0.10")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");


    @Test
    public void readFromPostgres() throws Exception {
        Configuration configuration = Configuration
                .create()
                .with("name", "postgres-inventory-connector")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                /* begin connector properties */
                .with("tasks.max", "1")
                .with("database.hostname", postgres.getContainerIpAddress())
                .with("database.port", postgres.getMappedPort(POSTGRESQL_PORT))
                .with("database.user", "postgres")
                .with("database.password", "postgres")
                .with("database.dbname", "postgres")
                .with("database.server.name", "dbserver1")
                .with("table.whitelist", "inventory.customers")
                .with("schema.whitelist", "inventory")
                .with("database.history.hazelcast.list.name", "test")
                .build();

        String[] expectedEvents = {
                "1001/0:SYNC:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                "1002/0:SYNC:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                "1003/0:SYNC:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                "1004/0:SYNC:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
        };

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(DebeziumSources.cdc(configuration))
                .withoutTimestamps()
                .mapUsingService(ServiceFactories.nonSharedService(context -> new Parser()), Parser::getChangeEventValue)
                .groupingKey(changeEventValue -> changeEventValue.getLatest(Customer.class).id)
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, changeEventValue) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = changeEventValue.getOperation();
                            Customer customer = changeEventValue.getLatest(Customer.class);
                            return customerId + "/" + count + ":" + operation + ":" + customer;
                        })
                .writeTo(AssertionSinks.assertCollectedEventually(30,
                        assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("debezium-connector-postgres.zip")));

        // when
        JetInstance jet = createJetMember();
        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        sleepAtLeastSeconds(10);
        // update record
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.setSchema("inventory");
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
            Assert.fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            Assert.assertTrue("Job was expected to complete with " +
                            "AssertionCompletedException, but completed with: " + e.getCause(),
                    errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }


}
