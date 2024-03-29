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

package com.hazelcast.jet.contrib.connect;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.apache.kafka.connect.data.Values;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConnectRandomIntIntegrationTest extends JetTestSupport {

    public static final int ITEM_COUNT = 10_000;

    @Ignore("This test fails because of resource upload permissions. Fix this test " +
            "after the default resource upload permissions are decided.")
    @Test
    public void readFromRandomSource() throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "random-source-connector");
        randomProperties.setProperty("connector.class", "sasakitoa.kafka.connect.random.RandomSourceConnector");
        randomProperties.setProperty("generator.class", "sasakitoa.kafka.connect.random.generator.RandomInt");
        randomProperties.setProperty("tasks.max", "1");
        randomProperties.setProperty("messages.per.second", "1000");
        randomProperties.setProperty("topic", "test");
        randomProperties.setProperty("task.summary.enable", "true");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(randomProperties))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("random-connector-1.0-SNAPSHOT.jar"))
                                      .getPath()
        );

        Job job = createHazelcastInstance().getJet().newJob(pipeline, jobConfig);

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


}
