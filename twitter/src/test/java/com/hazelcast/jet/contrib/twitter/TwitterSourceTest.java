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

package com.hazelcast.jet.contrib.twitter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.Json;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.annotation.NightlyTest;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import twitter4j.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@Category(NightlyTest.class)
public class TwitterSourceTest extends JetTestSupport {

    private HazelcastInstance hz;
    private Properties credentials;


    @Before
    public void setup() {
        hz = createJetMember().getHazelcastInstance();
        credentials = loadCredentials();
    }

    @Test
    public void testStream_withTermFilter() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<>(Arrays.asList("BTC", "ETH"));
        final StreamSource<String> twitterTestStream = TwitterSources.stream(
                credentials, () -> new StatusesFilterEndpoint().trackTerms(terms));
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withoutTimestamps()
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));

        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 20 tweets in 1 min.", list.size(), 20)));
        Job job = hz.getJet().newJob(pipeline);
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

    @Test
    public void testStream_userFilter() {
        Pipeline pipeline = Pipeline.create();
        List<Long> userIds = new ArrayList<>(
                Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
        final StreamSource<String> twitterTestStream = TwitterSources.stream(credentials,
                () -> new StatusesFilterEndpoint().followings(userIds));
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withoutTimestamps()
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 15 tweets in 1 min.",
                        list.size(), 15)));
        Job job = hz.getJet().newJob(pipeline);
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

    @Test
    public void testTimestampedStream_termFilter() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<>(Arrays.asList("San Mateo", "Brno", "London", "Istanbul"));

        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream(
                credentials, () -> new StatusesFilterEndpoint().trackTerms(terms));
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(0)
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 20 tweets in 1 min.",
                        list.size(), 20)));
        Job job = hz.getJet().newJob(pipeline);
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

    @Test
    public void testTimestampedStream_userFilter() {
        Pipeline pipeline = Pipeline.create();
        List<Long> userIds = new ArrayList<>(
                Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream(
                credentials, () -> new StatusesFilterEndpoint().followings(userIds));
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(1)
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 15 tweets in 1 min.",
                        list.size(), 15)));
        Job job = hz.getJet().newJob(pipeline);
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

    @Test
    public void testBatch() {
        Pipeline pipeline = Pipeline.create();
        String query = "Jet flies";
        BatchSource<Status> twitterSearch = TwitterSources.search(credentials, query);
        BatchStage<String> tweets = pipeline
                .readFrom(twitterSearch)
                .map(status -> "@" + status.getUser() + " - " + status.getText());
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 10 tweets in 1 minute.",
                        list.size(), 10)));
        Job job = hz.getJet().newJob(pipeline);
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

    void containsKey(String key, Properties credentials) {
        if (!credentials.containsKey(key) ||
                credentials.getProperty(key).equals("REPLACE_THIS")) {
            throw new IllegalArgumentException("The Twitter credentials," + key + ", should not be null.\n" +
                    "Set the parameter \"" +  key + "\" in twitter-security.properties OR " +
                    "Set the corresponding environment variable. (e.g. JET_TWITTER_CONNECTOR_CONSUMER_KEY" +
                    " for consumerKey.)");
        }
    }

    private Properties loadCredentials() {
        Properties credentials = new Properties();
        String consumerKey = System.getenv("JET_TWITTER_CONNECTOR_CONSUMER_KEY");
        String consumerSecret = System.getenv("JET_TWITTER_CONNECTOR_CONSUMER_SECRET");
        String token = System.getenv("JET_TWITTER_CONNECTOR_TOKEN");
        String tokenSecret = System.getenv("JET_TWITTER_CONNECTOR_TOKEN_SECRET");

        if (consumerKey != null || consumerSecret != null | token != null || tokenSecret != null) {
            credentials.put("consumerKey", consumerKey);
            credentials.put("consumerSecret", consumerSecret);
            credentials.put("token", token);
            credentials.put("tokenSecret", tokenSecret);
        } else {
            try {
                credentials.load(Thread.currentThread().getContextClassLoader()
                                       .getResourceAsStream("twitter-security.properties"));
            } catch (IOException e) {
                System.out.println("Exception is thrown while loading Twitter credentials from" +
                        "twitter-security.properties file");
                throw rethrow(e);
            }
        }
        containsKey("consumerKey", credentials);
        containsKey("consumerSecret", credentials);
        containsKey("token", credentials);
        containsKey("tokenSecret", credentials);
        return credentials;
    }
}
