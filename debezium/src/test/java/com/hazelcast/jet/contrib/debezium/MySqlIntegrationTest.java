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

import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import io.debezium.config.Configuration;
import io.debezium.serde.DebeziumSerdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Values;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import org.testcontainers.shaded.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class MySqlIntegrationTest extends JetTestSupport {

    @Rule
    public MySQLContainer mysql = new MySQLContainer("debezium/example-mysql")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");


    @Test
    public void readFromMySql() throws Exception {
        // given
        Configuration configuration = Configuration
                .create()
                .with("name", "mysql-inventory-connector")
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                /* begin connector properties */
                .with("database.hostname", mysql.getContainerIpAddress())
                .with("database.port", mysql.getMappedPort(MYSQL_PORT))
                .with("database.user", "debezium")
                .with("database.password", "dbz")
                .with("database.server.id", "184054")
                .with("database.server.name", "dbserver1")
                .with("database.whitelist", "inventory")
                .with("table.whitelist", "inventory.customers")
                .with("include.schema.changes", "false")
                .with("database.history.hazelcast.list.name", "test")
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(DebeziumSources.cdc(configuration))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .filterUsingService(ServiceFactories.sharedService(context -> {
                    Serde<EventRecord> serde = DebeziumSerdes.payloadJson(EventRecord.class);
                    serde.configure(Collections.emptyMap(), false);
                    return serde.deserializer();
                }, Deserializer::close), (deserializer, record) -> {
                    EventRecord eventRecord = deserializer.deserialize("", record.getBytes());
                    return eventRecord.isUpdate();
                })
                .writeTo(AssertionSinks.assertCollectedEventually(30,
                        list -> Assert.assertTrue(list.stream().anyMatch(s -> s.contains("Anne Marie")))));

        JobConfig jobConfig = new JobConfig();
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
            PreparedStatement preparedStatement = connection
                    .prepareStatement("UPDATE customers SET first_name='Anne Marie' WHERE id=1004;");
            preparedStatement.executeUpdate();
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

    private static final class EventRecord implements Serializable {

        @JsonIgnore
        public JsonNode source;

        @JsonIgnore
        public JsonNode after;

        @JsonIgnore
        public JsonNode before;

        @JsonIgnore
        public long ts_ms;

        @JsonProperty
        public String op;

        EventRecord() {
        }

        EventRecord(String operation) {
            this.op = operation;
        }

        public boolean isUpdate() {
            return "u".equals(op);
        }

        public boolean isCreate() {
            return "c".equals(op);
        }
    }


}
