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

package com.hazelcast.jet.contrib.mqtt;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class MqttSourcesTest extends JetTestSupport {

    private JetInstance jet;
    private List<String> inputList;

    @Before
    public void setUp() {
        jet = createJetMember();
        inputList = Arrays.asList("hello", "friend");
    }

    @Test
    public void testList() {

        Pipeline pipeline = Pipeline.create();

        BatchSource<String> testSource = MqttSources.testList(inputList);

        BatchStage<String> testStage = pipeline.readFrom(testSource);
        testStage.writeTo(AssertionSinks.assertCollected(s -> assertNotNull(s)));

        Job job = jet.newJob(pipeline);

        job.join();

    }
}
