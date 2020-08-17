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

package com.hazelcast.jet.testcontainers;

import com.hazelcast.collection.IList;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.ObjectStreamClass;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JetContainerClientTest {

    private static final Logger log = LoggerFactory.getLogger(JetContainerClientTest.class);

    @SuppressWarnings("DeclarationOrder")
    @Rule
    public JetContainer<?> jet = new JetContainer<>()
            .withHazelcastConfigurationFile("src/test/resources/hazelcast.yaml")
            .withJetConfigurationFile("src/test/resources/hazelcast-jet.yaml")
            .withJvmOptionsFile("src/test/resources/jvm.options")
            .withJetModule("kafka")
            .withJarOnClassPath("src/test/resources/empty.jar")
            //            .withDebugEnabled()
            ;

    @Before
    public void setUp() throws Exception {
        jet.followOutput(new Slf4jLogConsumer(log));
    }

    @Test
    public void shouldBeAbleToSubmitJob() {
        long uid = ObjectStreamClass.lookup(ConsumerEx.class).getSerialVersionUID();
        System.out.println(uid);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(42))
         .writeTo(Sinks.list("container-test-list"));


        JetInstance jetClient = jet.getJetClient();
        jetClient.newJob(p).join();

        IList<Integer> list = jetClient.getList("container-test-list");
        assertThat(list).containsExactly(42);
    }

    @Test
    public void shouldReturnClientConnectedToMember() {
        JetInstance client = jet.getJetClient();
        List<Job> jobs = client.getJobs();

        assertThat(jobs).isEmpty();
    }
}
