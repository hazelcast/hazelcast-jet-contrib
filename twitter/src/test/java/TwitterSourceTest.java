
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
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
import static java.util.concurrent.TimeUnit.SECONDS;
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
    public void it_should_read_from_twitter_source() {
        Pipeline pipeline = Pipeline.create();
//        List<Long> followings =  new ArrayList<Long>( Arrays.asList(3L, 5L, 3213L));
        List<String> terms = new ArrayList<String>(Arrays.asList("Twitter", "Api", "Test"));
        final StreamSource<String> twitterTestStream = TwitterSources.stream("twitter-test-source", ()-> new StatusesFilterEndpoint().trackTerms(terms), credentials);
        StreamStage<String> tweets = pipeline
                .readFrom(twitterTestStream)
                .withNativeTimestamps(SECONDS.toMillis(1));
        tweets.writeTo(AssertionSinks.assertCollectedEventually(60,
                list -> list.toString())); //
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
