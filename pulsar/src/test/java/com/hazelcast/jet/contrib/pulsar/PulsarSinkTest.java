/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.testcontainers.containers.PulsarContainer;

import java.util.concurrent.CompletableFuture;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.JetAssert.assertTrue;

public class PulsarSinkTest extends JetTestSupport {
    private static PulsarTestSupport pulsarTestSupport;
    private static final int ITEM_COUNT = 100_000;

    @Rule
    public PulsarContainer pulsarContainer = new PulsarContainer("2.5.0");

    private String sourceImapName = randomMapName();
    private String topic;
    private IMap<String, String> sourceIMap;
    private JetInstance jet;

    @Before
    public void setup() {
        pulsarTestSupport = new PulsarTestSupport(pulsarContainer.getPulsarBrokerUrl());
        pulsarContainer.start();
        jet = createJetMember();
        topic = pulsarTestSupport.getTopicName();
        sourceIMap = jet.getMap(sourceImapName);
        IntStream.range(0, ITEM_COUNT).forEach(i -> sourceIMap.put(String.valueOf(i), String.valueOf(i)));
    }

    @After
    public void after() {
        pulsarContainer.stop();
        jet.shutdown();
    }


    @AfterClass
    public static void afterClass() throws PulsarClientException {
        pulsarTestSupport.shutdown();
        pulsarTestSupport = null;
    }


    @Test
    public void testPulsarSink() throws PulsarClientException {
        Pipeline p = Pipeline.create();
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("maxPendingMessages", 5000);
        Sink<Integer> pulsarSink = PulsarSinks.<Integer, Integer>builder(topic,
                producerConfig,
                () -> PulsarClient.builder()
                                  .serviceUrl(pulsarTestSupport.getServiceUrl())
                                  .build())
                .schemaSupplier(() -> Schema.INT32)
                .extractValueFn(FunctionEx.identity())
                .build();
        p.readFrom(Sources.<String, String>map(sourceImapName))
         .map(x -> Integer.parseInt(x.getValue()))
         .writeTo(pulsarSink);
        jet.newJob(p)
           .join();
        sleepAtLeastSeconds(5);
        consumeMessages(ITEM_COUNT).thenRun(
                () -> {
                    try {
                        assertTrue("It should reach end of topic after consuming produced number of messages",
                                pulsarTestSupport.getConsumer()
                                                 .hasReachedEndOfTopic());
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                });
    }

    private CompletableFuture<Message<Integer>> consumeMessages(int count) throws PulsarClientException {
        CompletableFuture<Message<Integer>> last = null;
        for (int i = 0; i < count; i++) {
            last = pulsarTestSupport.consumeAsync();
        }
        return last;
    }
}
