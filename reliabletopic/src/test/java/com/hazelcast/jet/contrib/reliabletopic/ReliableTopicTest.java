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

package com.hazelcast.jet.contrib.reliabletopic;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;



public class ReliableTopicTest extends JetTestSupport {
    private static final int ITEM_COUNT = 550_000;
    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @Test
    public void reliableTopicTest() {
        String topicName = randomName();
        IList<Long> list = getList(jet);

        final StreamSource<Long> topicSrc = ReliableTopicSource.<Integer, Long>topicSource(topicName,
                msg -> Long.valueOf(msg.getMessageObject()));

        List<Integer> intNumbers = IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toList());

        Pipeline p1 = Pipeline.create();
        p1.readFrom(topicSrc)
          .withoutTimestamps()
          .writeTo(Sinks.list(list));

        Pipeline p2 = Pipeline.create();
        p2.readFrom(TestSources.items(intNumbers))
          .writeTo(Sinks.reliableTopic(topicName));

        Job job = jet.newJob(p1);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        jet.newJob(p2).join();

        assertTrueEventually(() -> {
            Assert.assertEquals("The same number of items should be read from ITopic source", ITEM_COUNT, list.size());
        }, 300);
    }
}
