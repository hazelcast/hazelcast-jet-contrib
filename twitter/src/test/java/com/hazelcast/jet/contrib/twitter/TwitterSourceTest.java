/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.internal.json.Json;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;

import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TwitterSourceTest extends JetTestSupport {

    private JetInstance jet;
    private Properties credentials;

    @Before
    public void setup() {
        jet = createJetMember();
        credentials = loadCredentialsFromEnv();
    }

    @After
    public void tearDown() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void it_should_read_from_twitter_stream_source_with_term_filter() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<String>(Arrays.asList("BTC", "ETH"));
        final StreamSource<String> twitterTestStream = TwitterSources.stream("twitter-test-source",
                () -> new StatusesFilterEndpoint().trackTerms(terms), credentials, Constants.STREAM_HOST);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withoutTimestamps()
                .map(rawJson -> Json.parse(rawJson)
                        .asObject()
                        .getString("text", null));

        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 20 tweets in 1 min.", list.size(), 20)));
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

    @Test
    public void it_should_read_from_twitter_stream_source_with_user_filter() {
        Pipeline pipeline = Pipeline.create();
        List<Long> userIds = new ArrayList<Long>(
                Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
        final StreamSource<String> twitterTestStream = TwitterSources.stream("twitter-test-source",
                () -> new StatusesFilterEndpoint().followings(userIds), credentials, Constants.STREAM_HOST);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withoutTimestamps()
                .map(rawJson -> Json.parse(rawJson)
                        .asObject()
                        .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
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

    @Test
    public void it_should_read_from_twitter_timestamped_stream_source_with_term_filter() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<String>(Arrays.asList("San Mateo", "Brno", "London", "Istanbul"));

        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream("twitter-test-source",
                () -> new StatusesFilterEndpoint().trackTerms(terms), credentials, Constants.STREAM_HOST);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(0)
                .map(rawJson -> Json.parse(rawJson)
                        .asObject()
                        .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 20 tweets in 1 min.",
                        list.size(), 20)));
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

    @Test
    public void it_should_read_from_twitter_timestamped_stream_source_with_user_filter() {
        Pipeline pipeline = Pipeline.create();
        List<Long> userIds = new ArrayList<Long>(
                Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream("twitter-test-source",
                () -> new StatusesFilterEndpoint().followings(userIds), credentials, Constants.STREAM_HOST);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(0)
                .map(rawJson -> Json.parse(rawJson)
                        .asObject()
                        .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> assertGreaterOrEquals("Emits at least 20 tweets in 1 min.",
                        list.size(), 20)));
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

    private static Properties loadCredentialsFromEnv() {
        Properties credentials = new Properties();
        credentials.put("consumerKey", System.getenv("JET_TWITTER_CONNECTOR_CONSUMER_KEY"));
        credentials.put("consumerSecret", System.getenv("JET_TWITTER_CONNECTOR_CONSUMER_SECRET"));
        credentials.put("token", System.getenv("JET_TWITTER_CONNECTOR_TOKEN"));
        credentials.put("tokenSecret", System.getenv("JET_TWITTER_CONNECTOR_TOKEN_SECRET"));
        return credentials;
    }

    private static Properties loadCredentialsFromConfigurationFile(){
        Properties credentials = new Properties();
        try {
            credentials.load(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("twitter-security.properties"));
        } catch (IOException e) {
            throw rethrow(e);
        }
        return credentials;
    }
}

