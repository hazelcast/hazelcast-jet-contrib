/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgreSqlIntegrationTest extends JetTestSupport {

    @Rule
    public PostgreSQLContainer postgres = new PostgreSQLContainer("debezium/example-postgres")
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
                .with("schema.whitelist", "inventory")
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
                                                          .getResource("debezium-connector-postgres.zip")));

        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // update record
        Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
        connection.setSchema("inventory");
        PreparedStatement preparedStatement = connection.prepareStatement("update customers set " +
                "first_name = 'Anne Marie' where id = 1004;");
        preparedStatement.executeUpdate();
        connection.close();

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }


    }
}
