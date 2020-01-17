
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
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
import static org.junit.Assert.*;

public class TwitterSourceTest extends JetTestSupport {

    private JetInstance jet;
    private Properties credentials;

    @Before
    public void setup() {
        jet = createJetMember();
        credentials = loadCredentials();
    }

    @After
    public void tearDown() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void it_should_read_from_twitter_stream_source() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<String>(Arrays.asList("BTC", "ETH"));
        final StreamSource<String> twitterTestStream = TwitterSources.stream("twitter-test-source", () -> new StatusesFilterEndpoint().trackTerms(terms), credentials, Constants.STREAM_HOST);
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
    public void it_should_read_from_twitter_stream_source_2() {
        Pipeline pipeline = Pipeline.create();
        List<String> terms = new ArrayList<String>(Arrays.asList("San Mateo", "Brno", "London", "Istanbul"));

        final StreamSource<String> twitterTestStream = TwitterSources.stream("twitter-test-source",
                () -> new StatusesFilterEndpoint().trackTerms(terms), credentials, Constants.STREAM_HOST);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withoutTimestamps()
                .map(rawJson -> Json.parse(rawJson)
                        .asObject()
                        .getString("text", null));
        tweets.writeTo(Sinks.logger());
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
    public void it_should_read_from_twitter_timestamped_stream_source() {
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
        tweets.writeTo(Sinks.logger());
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

    private static Properties loadCredentials() {
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
