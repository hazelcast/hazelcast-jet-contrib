package com.hazelcast.jet.contrib.itopic;

import com.hazelcast.internal.json.Json;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.topic.Message;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static com.hazelcast.test.HazelcastTestSupport.assertGreaterOrEquals;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;

class ITopicSourceTest {

    @Test
    public void itopic_test() {
        String topicName = randomName();
        final StreamSource<Long> topicSrc = ITopicSource.<Integer, Long>topicSource(topicName,
                msg -> msg.getMessageObject().longValue());
        Pipeline pipeline = Pipeline.create();

        List<Integer> userIdsAsInteger = new ArrayList<Integer>(
                Arrays.asList(612473, 759251, 1367531, 34713362, 51241574, 87818409));
        List<Long> userIdsAsLong = new ArrayList<Long>(
                Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));

        StreamStage<Long> userdIdsStream = pipeline
                .readFrom(topicSrc)
                .withNativeTimestamps(5);

        userdIdsStream.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 15 tweets in 1 min.",
                        list.size(), 15)));
        Job job = jet.newJob(pipeline);
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