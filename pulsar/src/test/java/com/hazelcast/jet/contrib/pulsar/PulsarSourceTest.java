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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;

import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PulsarSourceTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 1_000;

    @Test
    public void when_projectionFunctionProvided_thenAppliedToReadRecords() {
        String topicName = randomName();
        // Add a suffix to messages so that this projectionFn does a bit more than byte->String conversion.
        StreamSource<String> pulsarConsumerSrc = setupConsumerSource(topicName,
                x -> new String(x.getData(), StandardCharsets.UTF_8) + "-suffix");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
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
        Job job = createJetMember().newJob(pipeline);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        produceMessages("hello-pulsar", topicName, ITEM_COUNT);

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
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createJetMember().getHazelcastInstance());

        String topicName = randomName();
        StreamSource<String> pulsarConsumerSrc = setupConsumerSource(topicName,
                x -> new String(x.getData(), StandardCharsets.UTF_8));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSrc)
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
        Job job = instances[0].getJet().newJob(pipeline);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        produceMessages("hello-pulsar", topicName, ITEM_COUNT);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
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

    public void integrationTest(ProcessingGuarantee guarantee) {
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createJetMember().getHazelcastInstance());

        String topicName = randomName();
        StreamSource<String> pulsarReaderSrc = setupReaderSource(topicName,
                x -> new String(x.getData(), StandardCharsets.UTF_8));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarReaderSrc)
                .withoutTimestamps()
                .rebalance()
                .writeTo(Sinks.list("test-list"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(guarantee);
        jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = instances[0].getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        produceMessages("before-restart", topicName, 2 * ITEM_COUNT);

        Collection<Object> list = instances[0].getList("test-list");
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
            Long lastExecutionId = assertJobRunningEventually((JetInstance) instances[0].getJet(), job, null);
            instances[1].getLifecycleService().terminate();
            assertJobRunningEventually((JetInstance) instances[0].getJet(), job, lastExecutionId);

            produceMessages("after-restart", topicName, 2 * ITEM_COUNT);

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
