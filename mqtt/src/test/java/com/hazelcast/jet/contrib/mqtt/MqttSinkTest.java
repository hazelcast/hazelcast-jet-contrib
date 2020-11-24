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

package com.hazelcast.jet.contrib.mqtt;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.retry.RetryStrategies;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertTrue;

public class MqttSinkTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private MqttClient client;
    private String broker;

    @Before
    public void setup() throws MqttException {
        jet = createJetMember();
        createJetMember();
        broker = mosquittoContainer.connectionString();
    }

    @After
    public void teardown() throws MqttException {
        client.disconnect();
        client.close();
    }

    @Test
    public void test_retryStrategy() throws MqttException {
        ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>();
        client = client(new SubscriberCallback((topic, mes) -> map.put(byteArrayToInt(mes.getPayload()), topic)));

        int itemCount = 2800;
        int expectedItemLoss = 100;

        Pipeline p = Pipeline.create();

        Sink<Integer> sink = MqttSinks.builder().broker(broker).topic("topic")
                .connectOptionsFn(() -> {
                    MqttConnectOptions options = new MqttConnectOptions();
                    options.setMaxInflight(itemCount);
                    options.setAutomaticReconnect(true);
                    options.setCleanSession(false);
                    return options;
                })
                .retryStrategy(RetryStrategies.indefinitely(1000))
                .<Integer>messageFn(item -> {
                    MqttMessage message = new MqttMessage(intToByteArray(item));
                    message.setQos(2);
                    return message;
                }).build();

        p.readFrom(TestSources.items(range(0, itemCount).boxed().collect(toList())))
                .rebalance()
                .writeTo(sink);

        jet.newJob(p);

        assertTrueEventually(() -> assertTrue(map.size() > itemCount / 2));

        mosquittoContainer.fixMappedPort();
        mosquittoContainer.stop();
        mosquittoContainer.start();

        assertTrueEventually(() -> assertTrue(map.size() > itemCount - expectedItemLoss));
    }

    @Test
    public void test() throws MqttException {
        client = new MqttClient(broker, newUnsecureUuidString(), new ConcurrentMemoryPersistence());
        client.connect();

        int itemCount = 2800;
        AtomicInteger counter = new AtomicInteger();
        client.subscribe("topic", 2, (topic, message) -> counter.incrementAndGet());

        Pipeline p = Pipeline.create();

        Sink<Integer> sink =
                MqttSinks.builder().broker(broker).topic("topic")
                        .connectOptionsFn(() -> {
                            MqttConnectOptions options = new MqttConnectOptions();
                            options.setMaxInflight(itemCount);
                            return options;
                        })
                        .<Integer>messageFn(item -> {
                            MqttMessage message = new MqttMessage(intToByteArray(item));
                            message.setQos(2);
                            return message;
                        }).build();

        p.readFrom(TestSources.items(range(0, itemCount).boxed().collect(toList())))
                .rebalance()
                .writeTo(sink);

        jet.newJob(p).join();

        assertEqualsEventually(itemCount, counter);
    }


    private static byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    static int byteArrayToInt(byte[] data) {
        int val = 0;
        for (int i = 0; i < 4; i++) {
            val = val << 8;
            val = val | (data[i] & 0xFF);
        }
        return val;
    }

    private MqttClient client(SubscriberCallback callback) throws MqttException {
        MqttClient client = new MqttClient(broker, newUnsecureUuidString(), new ConcurrentMemoryPersistence());
        client.setCallback(callback);
        callback.client = client;
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setCleanSession(false);
        client.connect(connectOptions);
        return client;
    }

    private static final class SubscriberCallback implements MqttCallbackExtended {

        private final BiConsumerEx<String, MqttMessage> messageConsumer;
        private MqttClient client;

        private SubscriberCallback(BiConsumerEx<String, MqttMessage> messageConsumer) {
            this.messageConsumer = messageConsumer;
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            try {
                System.out.println("reconnect " + reconnect);
                client.subscribe("topic", 2);
            } catch (MqttException exception) {
                exception.printStackTrace();
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            messageConsumer.accept(topic, message);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    }
}
