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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.util.stream.Collectors.toList;

/**
 * todo add proper javadoc
 */
public class MqttSinkTest extends JetTestSupport {

    @Rule
    public MosquittoContainer mosquittoContainer = new MosquittoContainer();

    private JetInstance jet;
    private IMqttAsyncClient client;
    private String broker;

    @Before
    public void setup() throws MqttException {
        jet = createJetMember();

        broker = mosquittoContainer.connectionString();
        client = MqttSinks.client(broker, newUnsecureUuidString());
        client.connect().waitForCompletion();
    }

    @After
    public void teardown() throws MqttException {
        client.disconnect();
        client.close();
    }



    @Test
    public void test() throws MqttException {

        List<String> expected = IntStream.range(0, 100).mapToObj(String::valueOf).collect(toList());
        List<String> actual = Collections.synchronizedList(new ArrayList<>());
        client.subscribe("/topic", 2, (topic, message) -> {
            actual.add(new String(message.getPayload()));
        }).waitForCompletion();

        Pipeline p = Pipeline.create();

        p.readFrom(TestSources.items(expected))
         .writeTo(MqttSinks.publish(broker, "/topic"));

        jet.newJob(p).join();

        assertEqualsEventually(() -> actual, expected);
    }
}
