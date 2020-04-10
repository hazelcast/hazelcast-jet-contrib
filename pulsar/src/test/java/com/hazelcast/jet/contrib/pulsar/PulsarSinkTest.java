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

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;

import org.apache.pulsar.client.api.PulsarClientException;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;


public class PulsarSinkTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 1_000;

    @Test
    public void testPulsarSink() throws PulsarClientException {
        String topicName = randomName();
        Sink<Integer> pulsarSink = setupSink(topicName); // Its projection function -> Integer::doubleValue

        Pipeline p = Pipeline.create();
        List<Integer> numbers = IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toList());
        p.readFrom(TestSources.items(numbers))
         .writeTo(pulsarSink);

        createJetMember().newJob(p).join();
        List<Double> list = consumeMessages(topicName, ITEM_COUNT);

        assertTrueEventually(() -> {
            Assert.assertEquals(ITEM_COUNT, list.size());
            for (double i = 0; i < ITEM_COUNT; i++) {
                assertTrue("missing entry: " + i, list.contains(i));
            }
        }, 10);
    }
}
