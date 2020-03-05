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
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
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

import java.util.Arrays;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PulsarSourceTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 1_000;
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
    public void when_projectionFunctionProvided_thenAppliedToReadRecords() {
        String topicName = randomName();
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");

        StreamSource<String> pulsarTestStream = PulsarSources.pulsarConsumer(
                Collections.singletonList(topicName),
                2,
                consumerConfig,
                () -> PulsarClient.builder().serviceUrl(PulsarTestSupport.getServiceUrl()).build(),
                () -> Schema.BYTES,
                x -> new String(x.getData()) + "-suffix");
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarTestStream)
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertEquals("# of Emitted items should be equal to # of published items",
                                    ITEM_COUNT, list.size());
                            for (int i = 0; i < ITEM_COUNT; i++) {
                                String message = "hello-pulsar-" + i + "-suffix";
                                Assert.assertTrue("missing entry: " + message, list.contains(message));
                            }
                        })
                );
        Job job = jet.newJob(pipeline);
        sleepAtLeastSeconds(5);
        try {
            PulsarTestSupport.produceMessages("hello-pulsar", topicName, ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
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
    public void when_readFromPulsarConsumer_then_jobGetsAllPublishedMessages() {
        JetInstance[] instances = new JetInstance[3];
        Arrays.setAll(instances, i -> createJetMember());
        String topicName = randomName();

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
        StreamSource<String> pulsarTestStream = PulsarSources.pulsarConsumer(
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
                        list -> {
                            assertEquals("# of Emitted items should be equal to # of published items",
                                    ITEM_COUNT, list.size());
                            for (int i = 0; i < ITEM_COUNT; i++) {
                                String message = "hello-pulsar-" + i;
                                Assert.assertTrue("missing entry: " + message, list.contains(message));
                            }
                        })
                );
        Job job = jet.newJob(pipeline);
        sleepAtLeastSeconds(5);
        try {
            PulsarTestSupport.produceMessages("hello-pulsar", topicName, ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

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
    public void integrationTest_noSnapshotting() throws InterruptedException {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() throws InterruptedException {
        integrationTest(ProcessingGuarantee.EXACTLY_ONCE);
    }

    public void integrationTest(ProcessingGuarantee guarantee) throws InterruptedException {
        String topicName = randomName();
        JetInstance[] instances = new JetInstance[2];
        Arrays.setAll(instances, i -> createJetMember());

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("readerName", "hazelcast-jet-consumer");
        // Create the pulsar source tha reads from topicName
        final StreamSource<String> pulsarTestStream = PulsarSources.pulsarReader(
                topicName,
                readerConfig,
                () -> PulsarClient.builder().serviceUrl(PulsarTestSupport.getServiceUrl()).build(),
                () -> Schema.BYTES,
                x -> new String(x.getData()));

        // Create a list sink to collect the emitted items.
        Sink<String> listSink = SinkBuilder
                .sinkBuilder("list-source", c -> c.jetInstance().getList("test-list"))
                .<String>receiveFn(List::add)
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarTestStream)
                .withoutTimestamps()
                .writeTo(listSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(guarantee);
        jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = jet.newJob(pipeline, jobConfig);

        try {
            PulsarTestSupport.produceMessages("before-restart", topicName, 2 * ITEM_COUNT);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        sleepAtLeastSeconds(5);
        Collection<Object> list = jet.getHazelcastInstance().getList("test-list");
        assertTrueEventually(() -> {
            Assert.assertEquals(2 * ITEM_COUNT, list.size());
            for (int i = 0; i < 2 * ITEM_COUNT; i++) {
                String message = "before-restart-" + i;
                Assert.assertTrue("missing entry: " + message, list.contains(message));
            }
        }, 15);

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until a new snapshot appears
            JobRepository jr = new JobRepository(instances[0]);
            long currentMax = jr.getJobExecutionRecord(job.getId()).snapshotId();
            assertTrueEventually(() -> {
                JobExecutionRecord jobExecutionRecord = jr.getJobExecutionRecord(job.getId());
                assertNotNull("jobExecutionRecord == null", jobExecutionRecord);
                long newMax = jobExecutionRecord.snapshotId();
                Assert.assertTrue("no snapshot produced", newMax > currentMax);
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });
            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].getHazelcastInstance().getLifecycleService().terminate();
            Thread.sleep(500);
            try {
                PulsarTestSupport.produceMessages("after-restart", topicName, 2 * ITEM_COUNT);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
            assertTrueEventually(() -> {
                Assert.assertEquals(4 * ITEM_COUNT, list.size());
                for (int i = 0; i < 2 * ITEM_COUNT; i++) {
                    String message = "before-restart-" + i;
                    Assert.assertTrue("missing entry: " + message, list.contains(message));
                }
                for (int i = 0; i < 2 * ITEM_COUNT; i++) {
                    String message = "after-restart-" + i;
                    Assert.assertTrue("missing entry: " + message, list.contains(message));
                }
            }, 10);
        }
        assertFalse(job.getFuture().isDone());
        // cancel the job
        job.cancel();
        assertTrueEventually(() -> Assert.assertTrue(job.getFuture().isDone()));
    }
}
