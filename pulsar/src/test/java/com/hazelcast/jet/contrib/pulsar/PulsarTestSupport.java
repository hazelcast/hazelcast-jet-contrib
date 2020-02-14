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
package com.hazelcast.jet.contrib.pulsar;

import com.hazelcast.jet.core.JetTestSupport;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.ClassRule;
import org.testcontainers.containers.PulsarContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class PulsarTestSupport extends JetTestSupport {
    @ClassRule
    public static PulsarContainer pulsarContainer = new PulsarContainer("2.5.0");

    private static final String TOPIC_NAME = "jet-test-topic";
    private static final int QUEUE_CAPACITY = 1000;
    private static PulsarClient client;
    private static Consumer<Integer> consumer;
    private static Producer<byte[]> producer;

    protected static void shutdown() throws PulsarClientException {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (client != null) {
            client.close();
        }
        client = null;
        producer = null;
        consumer = null;
    }

    protected static String getServiceUrl() {
        return pulsarContainer.getPulsarBrokerUrl();
    }

    private static PulsarClient getClient() throws PulsarClientException {
        if (client == null) {
            client = PulsarClient.builder()
                    .serviceUrl(getServiceUrl())
                    .build();
        }
        return client;
    }

    private static Producer<byte[]> getProducer() throws PulsarClientException {
        if (producer == null) {
            producer = getClient()
                    .newProducer()
                    .topic(TOPIC_NAME)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                    .sendTimeout(10, TimeUnit.SECONDS)
                    .blockIfQueueFull(true)
                    .create();
        }
        return producer;
    }

    protected static void produceMessages(String message, int count) throws PulsarClientException {
        for (int i = 0; i < count; i++) {
            produceAsync(message);
        }
    }

    protected static CompletableFuture<MessageId> produceAsync(String message) throws PulsarClientException {
        return getProducer().sendAsync(message.getBytes());
    }

    protected static String getTopicName() {
        return TOPIC_NAME;
    }

    protected static CompletableFuture<Message<Integer>> consumeMessages(int count) throws PulsarClientException {
        CompletableFuture<Message<Integer>> last = null;
        for (int i = 0; i < count; i++) {
            last = PulsarTestSupport.consumeAsync();
        }
        return last;
    }

    protected static CompletableFuture<Message<Integer>> consumeAsync() throws PulsarClientException {
        return getConsumer().receiveAsync();
    }

    protected static Consumer<Integer> getConsumer() throws PulsarClientException {
        if (consumer == null) {
            consumer = getClient()
                    .newConsumer(Schema.INT32)
                    .topic(TOPIC_NAME)
                    .consumerName("hazelcast-jet-consumer")
                    .subscriptionName("hazelcast-jet-subscription")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .receiverQueueSize(QUEUE_CAPACITY)
                    .subscribe();
        }
        return consumer;
    }
}
