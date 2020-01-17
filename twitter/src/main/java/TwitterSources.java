import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;


/**
 * Contains methods for creating Twitter stream sources.
 */
public final class TwitterSources {

    private TwitterSources() {
    }

    /**
     * Creates a {@link StreamSource} which reads tweets from Twitter's Streaming API
     * for data ingestion to Jet pipelines.
     *
     * @param name             a descriptive name of this source.
     * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source.
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey", "consumerSecret", "token", "tokenSecret" keys.
     * @param host             a Twitter endpoint host that are defined in {@link com.twitter.hbc.core.Constants}
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> timestampedStream(@Nonnull String name,
                                                             @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier,
                                                             @Nonnull Properties credentials,
                                                             @Nonnull String host) {
        return SourceBuilder.timestampedStream(name, ignored -> new TwitterSourceContext(endpointSupplier, credentials, host))
                .fillBufferFn(TwitterSourceContext::fillTimestampedBuffer)
                .destroyFn(TwitterSourceContext::close)
                .build();
    }


    /**
     * Creates a {@link StreamSource} which reads tweets from Twitter's Streaming API
     * for data ingestion to Jet pipelines.
     *
     * @param name             a descriptive name of this source.
     * @param endpointSupplier a supplier function that supplies a Twitter StreamingEndpoint to connect to source.
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey", "consumerSecret", "token", "tokenSecret" keys.
     * @param host             a Twitter endpoint host that are defined in {@link com.twitter.hbc.core.Constants}
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> stream(@Nonnull String name,
                                              @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier,
                                              @Nonnull Properties credentials,
                                              @Nonnull String host) {
        return SourceBuilder.stream(name, ignored -> new TwitterSourceContext(endpointSupplier, credentials, host))
                .fillBufferFn(TwitterSourceContext::fillBuffer)
                .destroyFn(TwitterSourceContext::close)
                .build();
    }

    /**
     * A source context of Twitter
     */
    private static class TwitterSourceContext {

        private static final int QUEUE_CAPACITY = 1000;
        private static final int MAX_FILL_ELEMENTS = 250;

        private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        private final ArrayList<String> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final BasicClient client;

        /**
         * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source.
         * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey", "consumerSecret", "token", "tokenSecret" keys.
         * @param host             a Twitter endpoint host that are defined in {@link com.twitter.hbc.core.Constants}
         */
        private TwitterSourceContext(
                @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier,
                @Nonnull Properties credentials,
                @Nonnull String host
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
                    .hosts(host)
                    .endpoint(endpointSupplier.get())
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build();
            client.connect();
        }


        private void fillBuffer(SourceBuilder.SourceBuffer<String> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (String item : buffer) {
                sourceBuffer.add(item);
            }
            buffer.clear();
        }

        private void fillTimestampedBuffer(SourceBuilder.TimestampedSourceBuffer<String> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (String item : buffer) {
                JsonObject object = Json.parse(item).asObject();
                long timestamp = Long.parseLong(object.getString("timestamp_ms", "0"));
                sourceBuffer.add(item, timestamp);
            }
            buffer.clear();
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
