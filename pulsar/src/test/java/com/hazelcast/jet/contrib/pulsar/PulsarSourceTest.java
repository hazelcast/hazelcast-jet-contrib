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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;

import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;

public class PulsarSourceTest extends PulsarTestSupport {
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
    public void testStream() {
        try {
            PulsarTestSupport.produceMessages("hello-pulsar", ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("serviceUrl", PulsarTestSupport.getServiceUrl());

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
        consumerConfig.put("subscriptionType", SubscriptionType.Exclusive);

        final StreamSource<String> pulsarTestStream = PulsarSources.subscribe(
                Collections.singletonList(PulsarTestSupport.getTopicName()),
                consumerConfig,
                () -> PulsarClient.builder().serviceUrl(PulsarTestSupport.getServiceUrl()).build(),
                () -> Schema.BYTES,
                x -> new String(x.getData()));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarTestStream)
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(20,
                        list -> assertEquals("# of Emitted items should be equal to # of published items",
                                ITEM_COUNT, list.size())));
        Job job = jet.newJob(pipeline);
        sleepAtLeastSeconds(5);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }


}
