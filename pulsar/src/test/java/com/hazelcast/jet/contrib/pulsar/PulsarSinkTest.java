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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.JetAssert.assertTrue;

public class PulsarSinkTest extends PulsarTestSupport {

    private static final int ITEM_COUNT = 100_000;
    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @After
    public void after() {
        jet.shutdown();
    }


    @AfterClass
    public static void afterClass() throws PulsarClientException {
        PulsarTestSupport.shutdown();
    }


    @Test
    public void testPulsarSink() throws PulsarClientException {
        String topicName = randomName();
        String sourceImapName = randomMapName();
        IMap<String, String> sourceIMap;
        sourceIMap = jet.getMap(sourceImapName);
        IntStream.range(0, ITEM_COUNT).forEach(i -> sourceIMap.put(String.valueOf(i), String.valueOf(i)));

        Pipeline p = Pipeline.create();
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("maxPendingMessages", 15000);

        Sink<Integer> pulsarSink = PulsarSinks.<Integer, Double>builder(topicName,
                producerConfig,
                () -> PulsarClient.builder()
                                  .serviceUrl(PulsarTestSupport.getServiceUrl())
                                  .build())
                .schemaSupplier(() -> Schema.DOUBLE)
                .extractValueFn(Integer::doubleValue)
                .build();
        p.readFrom(Sources.<String, String>map(sourceImapName))
         .map(x -> Double.parseDouble(x.getValue()))
         .map(Double::intValue)
         .writeTo(pulsarSink);

        jet.newJob(p).join();

        sleepAtLeastSeconds(5);

        PulsarTestSupport.consumeMessages(topicName, ITEM_COUNT).thenRun(
                () -> {
                    try {
                        assertTrue("It should reach end of topic after consuming produced number of messages",
                                PulsarTestSupport.getConsumer(topicName).hasReachedEndOfTopic());
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                });
    }


}
