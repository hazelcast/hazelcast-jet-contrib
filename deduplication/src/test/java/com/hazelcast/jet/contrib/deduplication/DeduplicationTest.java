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

package com.hazelcast.jet.contrib.deduplication;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinkBuilder;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.util.UuidUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.contrib.deduplication.Deduplications.deduplicationWindow;
import static com.hazelcast.jet.pipeline.test.TestSources.itemStream;
import static org.junit.Assert.fail;

public final class DeduplicationTest {
    private static final long TEST_DURATION_NANOS = TimeUnit.SECONDS.toNanos(20);
    private static final long WINDOW_LENGTH = 10_000;
    private static final long ALLOWED_LAG = 0;
    private static final int  ITEMS_PER_SECOND = 100;

    @Test
    public void testDeduplication() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(itemStream(ITEMS_PER_SECOND, withOccasionalDuplicates()))
                .withNativeTimestamps(ALLOWED_LAG)
                .apply(deduplicationWindow(WINDOW_LENGTH))
                .drainTo(failOnDuplicatesSink());

        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();
        Job job = instance1.newJob(pipeline);

        assertNoDuplicates(job);
    }

    private static void assertNoDuplicates(Job job) {
        try {
            job.join();
            fail("Did not receive expected AssertionCompletedException");
        } catch (CompletionException e) {
            //expected
            String errorMsg = e.getCause().getMessage();
            Assert.assertTrue("Job was expected to complete with AssertionCompletedException,"
                            + "but completed with: " + e.getCause(),
                    errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    private static Sink<Object> failOnDuplicatesSink() {
        return AssertionSinkBuilder.assertionSink("fail-on-duplication",
                    () -> Tuple2.tuple2(new HashSet<>(), System.nanoTime() + TEST_DURATION_NANOS))
                    .receiveFn((s, i) -> {
                        HashSet<Object> set = s.f0();
                        Long deadline = s.f1();
                        if (!set.add(i)) {
                            throw new AssertionError("Element " + i + " was duplicated");
                        }
                        if (System.nanoTime() >= deadline) {
                            if (set.size() > 0) {
                                throw new AssertionCompletedException();
                            } else {
                                throw new AssertionError("No elements received by the sink. "
                                        + "Something is terribly wrong!");
                            }
                        }
                    }).build();
    }

    private static GeneratorFunction<UUID> withOccasionalDuplicates() {
        AtomicReference<UUID> lastUUID = new AtomicReference<>(UuidUtil.newUnsecureUUID());
        return (ts, seq) -> {
            if (ThreadLocalRandom.current().nextInt(10) != 0) {
                // pick a new UUID unless something improbable happens
                lastUUID.set(UuidUtil.newUnsecureUUID());
            }
            return lastUUID.get();
        };
    }
}
