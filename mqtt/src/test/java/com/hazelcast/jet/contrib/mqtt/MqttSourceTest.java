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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MqttSourceTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private IMqttAsyncClient client;
    private String broker;
    private IList<byte[]> sinkList;

    @Before
    public void setup() throws MqttException {
        jet = createJetMember();
        createJetMember();

        sinkList = jet.getList("sinkList");

        broker = mosquittoContainer.connectionString();
        client = createClient();

        client.publish("topic1", "retain".getBytes(), 2, true).waitForCompletion();
        client.publish("topic2", "retain".getBytes(), 2, true).waitForCompletion();
        client.publish("topic3", "retain".getBytes(), 2, true).waitForCompletion();
    }

    @After
    public void teardown() throws MqttException {
        client.disconnect().waitForCompletion();
        client.close();
    }

    @Test
    public void test_autoReconnect() throws MqttException {
        String topic = "topic1";

        Pipeline p = Pipeline.create();

        StreamSource<byte[]> source =
                MqttSources.builder()
                        .broker(broker)
                        .topic(topic)
                        .qualityOfService(EXACTLY_ONCE)
                        .autoReconnect()
                        .build();

        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        Job job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, 1);
        client.publish(topic, "message1".getBytes(), 2, false).waitForCompletion();
        assertEqualsEventually(sinkList::size, 2);

        mosquittoContainer.fixMappedPort();
        mosquittoContainer.stop();
        assertEquals(RUNNING, job.getStatus());
        mosquittoContainer.start();

        assertTrueEventually(() -> {
            if (client.isConnected()) {
                client.publish(topic, "message2".getBytes(), 2, false).waitForCompletion();
            }
            assertTrue(sinkList.size() > 2);
        });

        job.cancel();
    }

    @Test
    public void test() throws MqttException {
        int messageCount = 6000;
        Subscription[] subscriptions = new Subscription[]{
                Subscription.of("topic1", EXACTLY_ONCE),
                Subscription.of("topic2", EXACTLY_ONCE),
                Subscription.of("topic3", EXACTLY_ONCE),
        };
        Pipeline p = Pipeline.create();
        StreamSource<byte[]> source =
                MqttSources.builder()
                        .broker(broker)
                        .subscriptions(subscriptions)
                        .build();
        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        Job job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, subscriptions.length);

        List<IMqttDeliveryToken> list = new ArrayList<>();
        for (int i = 0; i < messageCount / 100; i++) {
            for (Subscription subscription : subscriptions) {
                for (int j = 0; j < 100; j++) {
                    IMqttDeliveryToken token = client.publish(subscription.getTopic(),
                            ("message" + i).getBytes(), subscription.getQualityOfService().getQos(), false);
                    list.add(token);
                }
            }
            for (IMqttDeliveryToken token : list) {
                token.waitForCompletion();
            }
            list.clear();
        }

        assertEqualsEventually(sinkList::size, (messageCount + 1) * subscriptions.length);

        job.cancel();
    }

    private MqttAsyncClient createClient() throws MqttException {
        MqttAsyncClient client = new MqttAsyncClient(broker, newUnsecureUuidString(),
                new ConcurrentMemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setMaxInflight(300);
        options.setAutomaticReconnect(true);
        client.connect(options).waitForCompletion();
        return client;
    }
}
