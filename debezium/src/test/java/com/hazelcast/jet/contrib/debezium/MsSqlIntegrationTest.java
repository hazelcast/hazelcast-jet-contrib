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
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.fail;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

public class MsSqlIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public MSSQLServerContainer mssql = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server")
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withClasspathResourceMapping("mssql/setup.sql",
                    "/tmp/setup.sql", BindMode.READ_ONLY)
            .withClasspathResourceMapping("mssql/cdc.sql",
                    "/tmp/cdc.sql", BindMode.READ_ONLY);

    @Test
    public void readFromMsSql() throws Exception {
        execInContainer("setup.sql");
        assertTrueEventually(() -> {
            Container.ExecResult result = execInContainer("cdc.sql");
            assertContains(result.getStdout(), "already");
        });

        Configuration configuration = Configuration
                .create()
                .with("name", "mssql-inventory-connector")
                .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                /* begin connector properties */
                .with("tasks.max", "1")
                .with("database.hostname", mssql.getContainerIpAddress())
                .with("database.port", mssql.getMappedPort(MS_SQL_SERVER_PORT))
                .with("database.user", mssql.getUsername())
                .with("database.password", mssql.getPassword())
                .with("database.dbname", "MyDB")
                .with("database.server.name", "fulfillment")
                .with("table.whitelist", "inventory.customers")
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
                .setLocalParallelism(1)
                .writeTo(AssertionSinks.assertCollectedEventually(30,
                        assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("debezium-connector-sqlserver.zip")));

        // when
        JetInstance jet = createJetMember();
        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        sleepAtLeastSeconds(10);
        // update record
        try (Connection connection = DriverManager.getConnection(mssql.getJdbcUrl() + ";databaseName=MyDB",
                mssql.getUsername(), mssql.getPassword())) {
            connection.setSchema("inventory");
            connection
                    .prepareStatement("UPDATE MyDB.inventory.customers SET first_name = 'Anne Marie' WHERE id = 1004")
                    .executeUpdate();
            connection
                    .prepareStatement("INSERT INTO MyDB.inventory.customers (first_name, last_name, email) " +
                            "VALUES ('Jason', 'Bourne', 'jason@bourne.org')")
                    .executeUpdate();
            connection
                    .prepareStatement("DELETE FROM MyDB.inventory.customers WHERE last_name='Bourne'")
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

    private Container.ExecResult execInContainer(String script) throws Exception {
        return mssql.execInContainer("/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", mssql.getUsername(),
                "-P", mssql.getPassword(), "-d", "master", "-i", "/tmp/" + script);
    }
}
