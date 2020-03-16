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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Values;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.fail;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

public class MsSqlIntegrationTest extends JetTestSupport {

    @Rule
    public MSSQLServerContainer mssql = new MSSQLServerContainer<>().withEnv("MSSQL_AGENT_ENABLED", "true")
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

        JetInstance jet = createJetMember();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(DebeziumSources.cdc(configuration))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertTrue(list.stream().anyMatch(s -> s.contains("Anne Marie")))));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("debezium-connector-sqlserver.zip")));

        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        sleepAtLeastSeconds(30);
        // update record
        try (Connection connection = DriverManager.getConnection(mssql.getJdbcUrl() + ";databaseName=MyDB",
                mssql.getUsername(), mssql.getPassword())) {
            connection.setSchema("inventory");
            PreparedStatement preparedStatement = connection.prepareStatement("update MyDB.inventory.customers set " +
                    "first_name = 'Anne Marie' where id = 1001;");
            preparedStatement.executeUpdate();
        }

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }


    }

    private Container.ExecResult execInContainer(String script) throws Exception {
        return mssql.execInContainer("/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", mssql.getUsername(),
                "-P", mssql.getPassword(), "-d", "master", "-i", "/tmp/" + script);
    }
}
