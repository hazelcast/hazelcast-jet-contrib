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
package com.hazelcast.jet.contrib.slack;

import com.hazelcast.function.Functions;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SlackSinksTest extends JetTestSupport {

    private JetInstance jet;
    private Properties credentials;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @Test
    public void testSlackSink() {
        List<String> text = jet.getList("text");
        text.add("hello world hello world hazelcast");
        text.add("sample message to slack channel");

        Pipeline pipeline = Pipeline.create();
        BatchStage<Map.Entry<String, Long>> tweets = pipeline
                .readFrom(Sources.<String>list("text"))
                .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty())
                .groupingKey(Functions.wholeItem())
                .aggregate(AggregateOperations.counting());

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list("text"))
                .writeTo(SlackSinks.sink(System.getenv("ACCESS_TOKEN"), System.getenv("CHANNEL_ID")));
        jet.newJob(p).join();
    }
}
