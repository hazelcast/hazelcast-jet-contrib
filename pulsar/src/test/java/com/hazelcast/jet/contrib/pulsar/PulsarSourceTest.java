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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;

import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PulsarSourceTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 250_000;
    private JetInstance jet;
    private JetInstance jetInstanceToShutdown;

    @Before
    public void setup() {
        jet = createJetMember();
        jetInstanceToShutdown = createJetMember();
    }

    @After
    public void after() {
        jet.shutdown();
        jetInstanceToShutdown.shutdown();

    }

    @AfterClass
    public static void afterClass() throws PulsarClientException {
        PulsarTestSupport.shutdown();
    }

    @Test
    public void testDistributedStream() {
        String topicName = randomName();
        try {
            PulsarTestSupport.produceMessages("hello-pulsar", topicName, ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
        StreamSource<String> pulsarTestStream = PulsarSources.pulsarDistributed(
                Collections.singletonList(topicName),
                2,
                consumerConfig,
                () -> PulsarClient.builder().serviceUrl(PulsarTestSupport.getServiceUrl()).build(),
                () -> Schema.BYTES,
                x -> new String(x.getData()));
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarTestStream)
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(60,
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


    @Test
    public void testFTStream() {
        String topicName = randomName();
        try {
            PulsarTestSupport.produceMessages("before-restart",  topicName, 2 * ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("readerName", "hazelcast-jet-consumer");
        final StreamSource<String> pulsarTestStream = PulsarSources.pulsarFT(
                topicName,
                readerConfig,
                () -> PulsarClient.builder().serviceUrl(PulsarTestSupport.getServiceUrl()).build(),
                () -> Schema.BYTES,
                x -> new String(x.getData()));
        Sink<String> listSink = SinkBuilder
                .sinkBuilder("list-source", c -> c.jetInstance().getList("test-list"))
                .<String>receiveFn(List::add)
                .build();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarTestStream)
                .withoutTimestamps()
                .writeTo(listSink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(5));
        Job job = jet.newJob(pipeline, jobConfig);

        sleepAtLeastSeconds(5);
        job.restart();
        try {
            PulsarTestSupport.produceMessages("after-restart", topicName, ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        sleepAtLeastSeconds(20);
        Collection<Object> list = jet.getHazelcastInstance().getList("test-list");
        assertTrueEventually(() -> Assert.assertEquals(3 * ITEM_COUNT, list.size()), 20);
        job.cancel();
    }


}
