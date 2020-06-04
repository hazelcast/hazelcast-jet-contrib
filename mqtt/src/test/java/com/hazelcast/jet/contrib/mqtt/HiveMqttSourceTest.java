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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.util.stream.Collectors.toList;

/**
 * todo add proper javadoc
 */
public class HiveMqttSourceTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private Mqtt5BlockingClient client;
    private String host;
    private int port;
    private IList<byte[]> sinkList;

    @Before
    public void setup() {
        jet = createJetMember();
        createJetMember();

        sinkList = jet.getList("sinkList");

        host = mosquittoContainer.host();
        port = mosquittoContainer.port();

        client = MqttClient.builder().useMqttVersion5()
                           .serverHost(host).serverPort(port)
                           .buildBlocking();

        client.connect();

        client.publishWith().topic("topic1").payload("retain".getBytes()).qos(MqttQos.EXACTLY_ONCE).retain(true).send();
        client.publishWith().topic("topic2").payload("retain".getBytes()).qos(MqttQos.EXACTLY_ONCE).retain(true).send();
        client.publishWith().topic("topic3").payload("retain".getBytes()).qos(MqttQos.EXACTLY_ONCE).retain(true).send();
    }

    @After
    public void teardown() {
        client.disconnect();
    }

    @Test
    public void test() {
        int messageCount = 6000;
        String[] topics = {"topic1", "topic2", "topic3"};
        List<Subscription> subscriptions = Arrays.stream(topics).map(t -> Subscription.of(t, 2)).collect(toList());
        Pipeline p = Pipeline.create();
        p.readFrom(HiveMqttSources.subscribe(host, port, newUnsecureUuidString(), subscriptions,
                () -> Mqtt5Connect.builder().cleanStart(false).build(), Mqtt5Publish::getPayloadAsBytes))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        Job job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, topics.length);


        Mqtt5AsyncClient asyncClient = client.toAsync();
        for (int i = 0; i < messageCount; i++) {
            for (String topic : topics) {
                asyncClient.publishWith().topic(topic).payload(("mes" + i).getBytes()).qos(MqttQos.EXACTLY_ONCE).send();
            }
        }

        assertEqualsEventually(sinkList::size, (messageCount + 1) * topics.length);

        job.cancel();
    }
}
