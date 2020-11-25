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

import com.hazelcast.jet.core.JetTestSupport;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PahoClientFaultToleranceTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private final String persistenceDir;

    {
        try {
            persistenceDir = Files.createTempDirectory("mqtt").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test_keepSession() throws Exception {
        String topic = "topic";
        int messageCount = 10_000;
        MessageCountingCallback callback = new MessageCountingCallback();
        MqttClient consumer = client("consumer", new MqttDefaultFilePersistence(persistenceDir));
        consumer.setCallback(callback);
        consumer.subscribe(topic, 2);

        spawn(() -> uncheckRun(() -> {
            MqttClient producer = client("producer", null);
            for (int i = 0; i < messageCount; i++) {
                producer.publish(topic, ("m-" + i).getBytes(), 2, false);
            }
            producer.disconnect();
            producer.close();
        }));

        assertTrueEventually(() -> assertTrue(callback.counter.get() > messageCount / 2));

        consumer.disconnect();
        consumer.close();

        consumer = client("consumer", new MqttDefaultFilePersistence(persistenceDir));
        consumer.setCallback(callback);
        consumer.subscribe(topic, 2);

        assertTrueEventually(() -> {
            System.out.println("messageCount=" + messageCount);
            assertEquals(messageCount, callback.counter.get());
        });
    }

    private MqttClient client(String clientId, MqttClientPersistence persistence) throws MqttException {
        MqttClient client = new MqttClient(mosquittoContainer.connectionString(), clientId, persistence);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        client.connect(options);
        return client;
    }

    static class MessageCountingCallback implements MqttCallback {

        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public void connectionLost(Throwable cause) {
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            counter.incrementAndGet();
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    }
}
