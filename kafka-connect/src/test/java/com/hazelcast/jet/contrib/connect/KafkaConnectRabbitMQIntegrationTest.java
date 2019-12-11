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

package com.hazelcast.jet.contrib.connect;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.connect.data.Values;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConnectRabbitMQIntegrationTest extends JetTestSupport {

    public static final int ITEM_COUNT = 10_000;

    @Rule
    public RabbitMQContainer rabbitMQContainer = new RabbitMQContainer()
            .withExchange("ex", "direct")
            .withQueue("test-queue")
            .withNetwork(Network.newNetwork());

    @Test
    public void readFromRabbitMqViaConnect() throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitMQContainer.getAdminUsername());
        factory.setPassword(rabbitMQContainer.getAdminPassword());
        factory.setHost(rabbitMQContainer.getContainerIpAddress());
        factory.setPort(rabbitMQContainer.getAmqpPort());

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueBind("test-queue", "ex", "test");
        byte[] messageBodyBytes = "Hello, world!".getBytes(StandardCharsets.UTF_8);

        publish(channel, messageBodyBytes, ITEM_COUNT);

        Properties rabbitMqProperties = new Properties();
        rabbitMqProperties.setProperty("name", "rabbitmq-source-connector");
        rabbitMqProperties.setProperty("connector.class", "com.github.jcustenborder" +
                ".kafka.connect.rabbitmq.RabbitMQSourceConnector");
        rabbitMqProperties.setProperty("kafka.topic", "messages");
        rabbitMqProperties.setProperty("rabbitmq.queue", "test-queue");
        rabbitMqProperties.setProperty("rabbitmq.host", rabbitMQContainer.getContainerIpAddress());
        rabbitMqProperties.setProperty("rabbitmq.port", rabbitMQContainer.getAmqpPort().toString());
        rabbitMqProperties.setProperty("rabbitmq.username", rabbitMQContainer.getAdminUsername());
        rabbitMqProperties.setProperty("rabbitmq.password", rabbitMQContainer.getAdminPassword());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(rabbitMqProperties))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("kafka-connect-rabbitmq-0.0.2-SNAPSHOT.zip"))
                                      .getPath()
        );

        Job job = createJetMember().newJob(pipeline, jobConfig);

        sleepAtLeastSeconds(5);
        publish(channel, messageBodyBytes, 1);
        channel.close();
        conn.close();

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    private void publish(Channel channel, byte[] messageBodyBytes, int itemCount) throws IOException {
        for (int i = 0; i < itemCount; i++) {
            channel.basicPublish("ex", "test", new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .contentEncoding(StandardCharsets.UTF_8.name())
                    .userId("guest")
                    .build(), messageBodyBytes);
        }
    }
}
