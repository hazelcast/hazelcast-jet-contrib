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

package com.hazelcast.jet.contrib.eventime;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinkBuilder;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public final class EventTimeUtilsTest extends JetTestSupport {
    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @Test
    public void smokeTest() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(TestSources.itemStream(1000))
                .withNativeTimestamps(0)
                .apply(EventTimeUtils.enrichWithEventTime())
                .map(e -> {
                    if (e.getEvent().timestamp() != e.getTimestamp()) {
                        throw new AssertionError("wrong timestamp attached");
                    }
                    return e;
                })
                .drainTo(assertItemsReceived(10_000));

        Job job = jet.newJob(pipeline);
        assertJobCompleted(job);
    }

    private static void assertJobCompleted(Job job) {
        try {
            job.join();
            fail("");
        } catch (CompletionException e) {
            // expected
        }
    }

    private static <T> Sink<T> assertItemsReceived(int itemCount) {
        return AssertionSinkBuilder.assertionSink("foo", AtomicInteger::new)
                    .<T>receiveFn((c, i) -> {
                        if (c.incrementAndGet() == itemCount) {
                            throw new AssertionCompletedException();
                        }
                    }).build();
    }

}