import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;

import org.json.JSONObject;


/**
 * Contains method for creating Twitter stream source.
 */
public final class TwitterSources {

    private TwitterSources() {
    }

    /**
     * @param name  a descriptive name of this source.
     * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source. e.g. StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
     *                                                                        List<Long> followings = Lists.newArrayList(1234L, 566788L);
     *                                                                        List<String> terms = Lists.newArrayList("twitter", "api");
     *                                                                        endpoint.followings(followings);
     *                                                                        endpoint.trackTerms(terms);
     * @param credentials a Twitter OAuth1 credentials that consists "consumerKey", "consumerSecret", "token", "tokenSecret" keys.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> stream(@Nonnull String name,
                                              @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier,
                                              @Nonnull Properties credentials) {
        return SourceBuilder.timestampedStream(name, ignored -> new TwitterSourceContext(endpointSupplier, credentials))
                .fillBufferFn(TwitterSourceContext::fillBuffer)
                .destroyFn(TwitterSourceContext::close)
                .build();
    }


    /**
     * A source context of Twitter
     */
    private static class TwitterSourceContext{

        private static final int QUEUE_CAPACITY = 10000;
        private static final int MAX_FILL_ELEMENTS = 500;

        private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        private final ArrayList<String> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final BasicClient client;

        /**
         * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source. e.g. StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
         *                                                                List<Long> followings = Lists.newArrayList(1234L, 566788L);
         *                                                                List<String> terms = Lists.newArrayList("twitter", "api");
         *                                                                endpoint.followings(followings);
         *                                                                endpoint.trackTerms(terms);
         * @param credentials a Twitter OAuth1 credentials that consists "consumerKey", "consumerSecret", "token", "tokenSecret" keys.
         */
        private TwitterSourceContext(
                @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier,
                @Nonnull Properties credentials
        ) {

            String consumerKey = credentials.getProperty("consumerKey");
            String consumerSecret = credentials.getProperty("consumerSecret");
            String token = credentials.getProperty("token");
            String tokenSecret = credentials.getProperty("tokenSecret");

            if (isMissing(consumerKey) || isMissing(consumerSecret) || isMissing(token) || isMissing(tokenSecret)) {
                throw new IllegalArgumentException("Twitter credentials are missing!");
            }

            Authentication auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
            client = new ClientBuilder()
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpointSupplier.get())
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build();
            client.connect();
        }


        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<String> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (String tweet : buffer) {
                JSONObject object = new JSONObject(tweet);
                if (object.has("text") && object.has("timestamp_ms")) {
                    String text = object.getString("text");
                    long timestamp = object.getLong("timestamp_ms");
                    sourceBuffer.add(text, timestamp);
                }
            }
            buffer.clear();

            if (queue.isEmpty()) {
                sourceBuffer.close();
            }
        }

        private void close() {
            if (client != null) {
                client.stop();
            }
        }

        private static boolean isMissing(String test) {
            return test.isEmpty() || "REPLACE_THIS".equals(test);
        }

    }

}
