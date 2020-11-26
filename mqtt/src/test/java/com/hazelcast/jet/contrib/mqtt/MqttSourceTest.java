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
import com.hazelcast.jet.contrib.mqtt.impl.IMapClientPersistence;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MqttSourceTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private MqttClient client;
    private String broker;
    private IList<byte[]> sinkList;
    private Job job;

    @Before
    public void setup() throws MqttException {
        jet = createJetMember();
        createJetMember();

        sinkList = jet.getList("sinkList");

        broker = mosquittoContainer.connectionString();
        client = createClient();

        client.publish("topic1", "retain".getBytes(), 2, true);
        client.publish("topic2", "retain".getBytes(), 2, true);
        client.publish("topic3", "retain".getBytes(), 2, true);
    }

    @After
    public void teardown() throws MqttException {
        if (job != null) {
            job.cancel();
        }
        if (client != null) {
            client.disconnect();
            client.close();
        }
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

        job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, 1);
        client.publish(topic, "message1".getBytes(), 2, false);
        assertEqualsEventually(sinkList::size, 2);

        mosquittoContainer.fixMappedPort();
        mosquittoContainer.stop();
        assertEquals(RUNNING, job.getStatus());
        mosquittoContainer.start();

        assertTrueEventually(() -> assertTrue(client.isConnected()));
        client.publish(topic, "message2".getBytes(), 2, false);
        assertTrueEventually(() -> assertTrue(sinkList.size() > 2));
    }

    @Test
    public void test() throws MqttException {
        int messageCount = 100;
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

        job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, subscriptions.length);

        for (int i = 0; i < messageCount; i++) {
            for (Subscription sub : subscriptions) {
                client.publish(sub.getTopic(), ("message" + i).getBytes(), sub.getQualityOfService().getQos(), false);
            }
        }

        assertEqualsEventually(sinkList::size, (messageCount + 1) * subscriptions.length);
    }

    private MqttClient createClient() throws MqttException {
        String clientId = newUnsecureUuidString();
        MqttClient client = new MqttClient(broker, clientId, new IMapClientPersistence(jet.getMap(clientId)));
        MqttConnectOptions options = new MqttConnectOptions();
        options.setMaxInflight(1_000);
        options.setAutomaticReconnect(true);
        options.setCleanSession(false);
        client.connect(options);
        return client;
    }
}
