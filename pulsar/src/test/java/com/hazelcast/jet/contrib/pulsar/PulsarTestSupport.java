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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class PulsarTestSupport extends JetTestSupport {
    @ClassRule
    public static PulsarContainer pulsarContainer = new PulsarContainer("2.5.0");
    private static final int QUEUE_CAPACITY = 1000;
    private static PulsarClient client;

    private static Map<String, Producer<byte[]>> producerMap = new HashMap<>();
    private static Map<String, Consumer<Double>> integerConsumerMap = new HashMap<>();

    protected static void shutdown() throws PulsarClientException {
        producerMap.forEach((s, producer) -> {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        integerConsumerMap.forEach((s, consumer) -> {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });

        if (client != null) {
            client.close();
        }
        client = null;
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

    private static Producer<byte[]> getProducer(String topicName) throws PulsarClientException {
        // If there exists a producer with same name returns it.
        if (!producerMap.containsKey(topicName)) {
            Producer<byte[]> newProducer = getClient()
                    .newProducer()
                    .topic(topicName)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                    .sendTimeout(10, TimeUnit.SECONDS)
                    .blockIfQueueFull(true)
                    .create();
            producerMap.put(topicName, newProducer);
            return newProducer;
        } else {
            return producerMap.get(topicName);
        }
    }

    protected static void produceMessages(String message, String topicName, int count) throws PulsarClientException {
        for (int i = 0; i < count; i++) {
            produceAsync(message + "-" + i, topicName);
        }
    }

    protected static CompletableFuture<MessageId> produceAsync(String message, String topicName)
            throws PulsarClientException {
        return getProducer(topicName).sendAsync(message.getBytes(StandardCharsets.UTF_8));
    }


    protected static CompletableFuture<Message<Double>> consumeMessages(String topicName, int count)
            throws PulsarClientException {
        CompletableFuture<Message<Double>> last = null;
        for (int i = 0; i < count; i++) {
            last = PulsarTestSupport.consumeAsync(topicName);
        }
        return last;
    }

    protected static CompletableFuture<Message<Double>> consumeAsync(String topicName) throws PulsarClientException {
        return getConsumer(topicName).receiveAsync();
    }

    protected static Consumer<Double> getConsumer(String topicName) throws PulsarClientException {
        if (!integerConsumerMap.containsKey(topicName)) {
            Consumer<Double> newConsumer = getClient()
                    .newConsumer(Schema.DOUBLE)
                    .topic(topicName)
                    .consumerName("hazelcast-jet-consumer-" + topicName)
                    .subscriptionName("hazelcast-jet-subscription")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .receiverQueueSize(QUEUE_CAPACITY)
                    .subscribe();
            integerConsumerMap.put(topicName, newConsumer);
            return newConsumer;
        } else {
            return integerConsumerMap.get(topicName);
        }
    }
}
