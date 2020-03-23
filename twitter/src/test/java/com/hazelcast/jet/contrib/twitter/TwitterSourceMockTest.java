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
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TwitterSourceMockTest extends JetTestSupport {

    @Rule
    public final MockWebServer server = new MockWebServer();

    @Before
    public void setUp() {
        prepareBatchMockResponse();
        prepareStreamingMockResponse();
    }

    @After
    public void tearDown() {
        System.clearProperty("twitter4j.restBaseURL");
    }

    @Test
    public void streamApiMockTest() {

        List<String> terms = new ArrayList<>(Arrays.asList("San Mateo", "Brno", "London", "Istanbul"));
        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream(getCredentials(),
                "http://" + server.getHostName() + ":" + server.getPort(),
                () -> new StatusesFilterEndpoint().trackTerms(terms));

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(0)
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(10,
                list -> assertGreaterOrEquals("Emits at least 100 tweets in 1 min.",
                        list.size(), 100)));
        Job job = createJetMember().newJob(pipeline);

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
    public void testBatchMock() {
        String baseUrl = "http://" + server.getHostName() + ":" + server.getPort() + "/";
        System.setProperty("twitter4j.restBaseURL", baseUrl);

        Pipeline pipeline = Pipeline.create();
        String query = "Jet flies";
        BatchSource<Status> twitterSearch = TwitterSources.search(
                getCredentials(), query);
        BatchStage<String> tweets = pipeline
                .readFrom(twitterSearch)
                .map(status -> "@" + status.getUser().getName() + " - " + status.getText());

        tweets.writeTo(AssertionSinks.assertCollectedEventually(10,
                list -> assertGreaterOrEquals("Emits at least 30 tweets in 10 secs.",
                        list.size(), 30)));
        Job job = createJetMember().newJob(pipeline);

        sleepAtLeastSeconds(2);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    /*
       The expected type of response is in the form like below:
       HTTP/1.1 200 OK
       Content-Type: text/plain
       Transfer-Encoding: chunked

       7\r\n
       Mozilla\r\n
       9\r\n
       Developer\r\n
       7\r\n
       Network\r\n
       0\r\n
       \r\n
    */
    private void prepareStreamingMockResponse() {
        String responseText = loadResponseFromFile("stream-response.json");
        StringBuilder responseBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            responseBuilder.append(responseText.length());
            responseBuilder.append("\r\n");
            responseBuilder.append(responseText);
            responseBuilder.append("\r\n");
        }
        responseBuilder.append("\r\n");
        String response = responseBuilder.toString();
        stubMockResponse(response);
    }

    private void prepareBatchMockResponse() {
        stubMockResponse(loadResponseFromFile("search-response1.json"));
        stubMockResponse(loadResponseFromFile("search-response2.json"));
    }

    private String loadResponseFromFile(String s) {
        return new Scanner(Objects.requireNonNull(
                Thread.currentThread()
                      .getContextClassLoader()
                      .getResourceAsStream(s)), "UTF-8").useDelimiter("\\A").next();
    }

    private void stubMockResponse(String response) {
        MockResponse mockResponse = new MockResponse()
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .setResponseCode(200)
                .setChunkedBody(response, 4096);
        server.enqueue(mockResponse);
    }

    private Properties getCredentials() {
        Properties credentials = new Properties();
        credentials.put("consumerKey", "mock_consumerKey");
        credentials.put("consumerSecret", "mock_consumerSecret");
        credentials.put("token", "mock_token");
        credentials.put("tokenSecret", "mock_tokenSecret");
        return  credentials;
    }

}
