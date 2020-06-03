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
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;

/**
 * todo add proper javadoc
 */
public class MqttSourceTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private IMqttClient client;
    private String broker;
    private IList<MqttMessage> sinkList;

    @Before
    public void setup() throws MqttException {
        jet = createJetMember();
        createJetMember();

        sinkList = jet.getList("sinkList");

        broker = mosquittoContainer.connectionString();
        client = MqttSources.client(broker, newUnsecureUuidString());
        client.connect();

        client.publish("/topic1", "retain".getBytes(), 2, true);
        client.publish("/topic2", "retain".getBytes(), 2, true);
        client.publish("/topic3", "retain".getBytes(), 2, true);
    }

    @After
    public void teardown() throws MqttException {
        client.disconnect();
        client.close();
    }

    @Test
    public void test() throws MqttException {
        int messageCount = 100;
        String[] topics = {"/topic1", "/topic2", "/topic3"};
        Pipeline p = Pipeline.create();
        p.readFrom(MqttSources.subscribe(broker, newUnsecureUuidString(), topics, MqttConnectOptions::new, (t, m) -> m))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        Job job = jet.newJob(p);

        assertEqualsEventually(sinkList::size, topics.length);

        for (int i = 0; i < 100; i++) {
            for(String topic: topics) {
                client.publish(topic, ("mes" + i).getBytes(), 2, false);
            }
        }

        assertEqualsEventually(sinkList::size, (messageCount+1)*topics.length);

        job.cancel();
    }
}
