package com.hazelcast.jet.contrib.twitter;

import com.hazelcast.internal.json.Json;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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


    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
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
    @Test
    public void streamApiMockTest() {
        String responseText = new Scanner(Objects.requireNonNull(
                Thread.currentThread()
                      .getContextClassLoader()
                      .getResourceAsStream("stream-response.json")), "UTF-8").useDelimiter("\\A").next();

        StringBuilder responseBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            responseBuilder.append(responseText.length());
            responseBuilder.append("\r\n");
            responseBuilder.append(responseText);
            responseBuilder.append("\r\n");
        }
        responseBuilder.append("\r\n");
        String response = responseBuilder.toString();

        Properties credentials = new Properties();
        credentials.put("consumerKey", "mock_consumerKey");
        credentials.put("consumerSecret", "mock_consumerSecret");
        credentials.put("token", "mock_token");
        credentials.put("tokenSecret", "mock_tokenSecret");
        MockResponse mockResponse = new MockResponse()
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .addHeader("Transfer-encoding", "chunked")
                .setResponseCode(200)
                .setChunkedBody(response, 4096);
        server.enqueue(mockResponse);

        Pipeline pipeline = Pipeline.create();

        List<String> terms = new ArrayList<>(Arrays.asList("San Mateo", "Brno", "London", "Istanbul"));

        final StreamSource<String> twitterTestStream = TwitterSources.timestampedStream(credentials,
                "http://" + server.getHostName() + ":" + server.getPort(),
                () -> new StatusesFilterEndpoint().trackTerms(terms));

        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(0)
                .map(rawJson -> Json.parse(rawJson)
                                    .asObject()
                                    .getString("text", null));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(10,
                list -> assertGreaterOrEquals("Emits at least 100 tweets in 1 min.",
                        list.size(), 100)));
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
