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
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MqttSourcesTest extends JetTestSupport {

    private JetInstance jet;
    private List<String> inputList;
    String uri = "https://jsonplaceholder.typicode.com/posts/1";

    @Before
    public void setUp() {
        jet = createJetMember();
        inputList = Arrays.asList("hello", "friend");
    }

    @Test
    public void testListBatchSource() {

        Pipeline pipeline = Pipeline.create();

        BatchSource<String> testSource = MqttSources.testListBatch(inputList);

        BatchStage<String> testStage = pipeline.readFrom(testSource);
        testStage.writeTo(AssertionSinks.assertCollected(s -> assertNotNull(s)));

        Job job = jet.newJob(pipeline);

        job.join();

    }

    @Test
    public void testListStreamSource() {

        Pipeline pipeline = Pipeline.create();

        StreamSource<String> testSource = MqttSources.testListStream(inputList);

        StreamStage<String> testStage = pipeline.readFrom(testSource).withoutTimestamps();
        testStage.writeTo(AssertionSinks.assertCollectedEventually(5, s -> assertNotNull(s)));

        Job job = jet.newJob(pipeline);

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
    public void testUrlStreamSource() {

        Pipeline pipeline = Pipeline.create();

        StreamSource<String> testSource = MqttSources.testUrlStream(uri);

        StreamStage<String> testStage = pipeline.readFrom(testSource).withoutTimestamps();
        testStage.peek(String::toUpperCase).writeTo(AssertionSinks.assertCollectedEventually(5, s -> assertNotNull(s)));

        Job job = jet.newJob(pipeline);

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
