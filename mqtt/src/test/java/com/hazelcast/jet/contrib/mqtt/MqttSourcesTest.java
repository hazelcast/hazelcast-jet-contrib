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

import static org.junit.Assert.*;

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