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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Contains methods for creating Twitter stream sources.
 */
public final class TwitterSources {

    private TwitterSources() {
    }

    /**
     * Creates a {@link StreamSource} which reads tweets using Twitter's
     * Streaming API for data ingestion to Jet pipelines. This method uses the
     * {@link com.twitter.hbc.core.Constants#STREAM_HOST} as a Twitter
     * Streaming API host.
     * <p>
     * Example usage:
     * <pre>{@code
     * Properties credentials = loadTwitterCredentials();
     * List<String> terms = new ArrayList<>(Arrays.asList("BTC", "ETH"));
     * StreamSource<String> streamSource =
     *              TwitterSources.stream(
     *                      credentials,
     *                      () -> new StatusesFilterEndpoint().trackTerms(terms)
     *              );
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<String> srcStage = p.readFrom(streamSource);
     * }</pre>
     *
     * @param credentials      a Twitter OAuth1 credentials that consist of
     *                         "consumerKey", "consumerSecret", "token",
     *                         "tokenSecret" keys.
     * @param endpointSupplier a supplier function that supplies a Twitter
     *                         StreamingEndpoint to connect to
     * @return a stream source to use in {@link Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> stream(
            @Nonnull Properties credentials,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return stream(credentials, Constants.STREAM_HOST, endpointSupplier);
    }

    /**
     * Equivalent of {@link #stream(Properties, SupplierEx)} with the
     * additional option to specify the Twitter {@code host}.
     *
     * @param host a Twitter host URL to connect to. These hosts are defined in
     *          {@link com.twitter.hbc.core.Constants}.
     */
    @Nonnull
    public static StreamSource<String> stream(
            @Nonnull Properties credentials,
            @Nonnull String host,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return SourceBuilder.stream("twitter-stream-source",
                ctx -> new TwitterStreamSourceContext(ctx.logger(), credentials, host, endpointSupplier))
                            .fillBufferFn(TwitterStreamSourceContext::fillBuffer)
                            .destroyFn(TwitterStreamSourceContext::close)
                            .build();
    }

    /**
     * Variant of {@link #stream(Properties, SupplierEx)} which parses the
     * messages and used the {@code timestamp_ms} field as a native timestamp.
     * If you don't need this timestamp, prefer to use the other variant.
     * <p>
     * For parameter documentation {@linkplain #stream(Properties,
     * SupplierEx) see here}.
     */
    @Nonnull
    public static StreamSource<String> timestampedStream(
            @Nonnull Properties credentials,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return timestampedStream(credentials, Constants.STREAM_HOST, endpointSupplier);
    }

    /**
     * Equivalent for {@link #timestampedStream(Properties, SupplierEx)} with
     * the additional option to specify the Twitter {@code host}.
     *
     * @param host a Twitter host URL to connect to. These hosts are defined in
     *          {@link com.twitter.hbc.core.Constants}.
     */
    @Nonnull
    public static StreamSource<String> timestampedStream(
            @Nonnull Properties credentials,
            @Nonnull String host,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return SourceBuilder.timestampedStream("twitter-timestamped-stream-source",
                ctx -> new TwitterStreamSourceContext(ctx.logger(), credentials, host, endpointSupplier))
                            .fillBufferFn(TwitterStreamSourceContext::fillTimestampedBuffer)
                            .destroyFn(TwitterStreamSourceContext::close)
                            .build();
    }

    /**
     * Creates a {@link BatchSource} which emits tweets in the form of {@link
     * Status} by using Twitter's Search API for data ingestion. Twitter
     * restricts the repeated (continuous) access to its search endpoint so you
     * can only make 180 calls every 15 minutes. This source tries to get the
     * search results from the search endpoint until the API rate limit is
     * exhausted.
     * <p>
     * Example usage:
     *
     * <pre>{@code
     *     Properties credentials = loadTwitterCredentials();
     *     BatchSource<Status> twitterSearchSource =
     *         TwitterSources.search(credentials,"Jet flies");
     *     Pipeline p = Pipeline.create();
     *     BatchStage<Status> srcStage = p.readFrom(twitterSearchSource);
     * }</pre>
     *
     * @param credentials a Twitter OAuth1 credentials that consists of
     *                    "consumerKey", "consumerSecret", "token",
     *                    "tokenSecret" keys.
     * @param query       a search query
     * @return a batch source to use in {@link Pipeline#readFrom}
     *
     * @see <a href="https://developer.twitter.com/en/docs/basics/rate-limiting">Twitter's Rate Limiting.</a>
     * @see <a href="https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets">
     *      Search tweets/ Twitter Developers</a>
     * @see <a href="https://developer.twitter.com/en/docs/tweets/search/guides/standard-operators">
     *      Twitter API / Standard search Operators</a>
     */
    @Nonnull
    public static BatchSource<Status> search(
            @Nonnull Properties credentials,
            @Nonnull String query
    ) {
        return SourceBuilder.batch("twitter-search-batch-source",
                ctx -> new TwitterBatchSourceContext(credentials, query))
                            .fillBufferFn(TwitterBatchSourceContext::fillBuffer)
                            .build();
    }

    private static void checkTwitterCredentials(Properties credentials) {
        String consumerKey = credentials.getProperty("consumerKey");
        String consumerSecret = credentials.getProperty("consumerSecret");
        String token = credentials.getProperty("token");
        String tokenSecret = credentials.getProperty("tokenSecret");

        Objects.requireNonNull(consumerKey, "consumerKey");
        Objects.requireNonNull(consumerSecret, "consumerSecret");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(tokenSecret, "tokenSecret");
    }

    /**
     * A context for the stream source of Twitter's Stream API
     */
    private static final class TwitterStreamSourceContext {

        private static final int QUEUE_CAPACITY = 1000;
        private static final int MAX_FILL_ELEMENTS = 250;

        private final ILogger logger;
        private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        private final List<String> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final BasicClient client;

        private TwitterStreamSourceContext(
                @Nonnull ILogger logger,
                @Nonnull Properties credentials,
                @Nonnull String host,
                @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
        ) {
            this.logger = logger;
            checkTwitterCredentials(credentials);
            String consumerKey = credentials.getProperty("consumerKey");
            String consumerSecret = credentials.getProperty("consumerSecret");
            String token = credentials.getProperty("token");
            String tokenSecret = credentials.getProperty("tokenSecret");

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
                try {
                    JsonObject object = Json.parse(item).asObject();
                    String timestampStr = object.getString("timestamp_ms", null);
                    if (timestampStr != null) {
                        long timestamp = Long.parseLong(timestampStr);
                        sourceBuffer.add(item, timestamp);
                    } else {
                        logger.warning("The tweet doesn't contain 'timestamp_ms' field\n" + item);
                    }
                } catch (Exception e) {
                    logger.warning("Error getting 'timestamp_ms' field from the tweet: " + e + "\n" + item, e);
                }
            }
            buffer.clear();
        }

        private void close() {
            if (client != null) {
                client.stop();
            }
        }
    }

    /**
     * Context for the batch source of Twitter's Search API
     */
    private static final class TwitterBatchSourceContext {
        private final Twitter twitter4JClient;
        private QueryResult searchResult;

        private TwitterBatchSourceContext(
                @Nonnull Properties credentials,
                @Nonnull String query
        ) throws TwitterException {
            checkTwitterCredentials(credentials);
            String consumerKey = credentials.getProperty("consumerKey");
            String consumerSecret = credentials.getProperty("consumerSecret");
            String token = credentials.getProperty("token");
            String tokenSecret = credentials.getProperty("tokenSecret");
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
              .setOAuthConsumerKey(consumerKey)
              .setOAuthConsumerSecret(consumerSecret)
              .setOAuthAccessToken(token)
              .setOAuthAccessTokenSecret(tokenSecret);
            this.twitter4JClient = new TwitterFactory(cb.build())
                    .getInstance();
            this.searchResult = twitter4JClient.search(new Query(query));
        }

        private void fillBuffer(SourceBuilder.SourceBuffer<Status> sourceBuffer) throws TwitterException {
            if (searchResult != null) {
                List<Status> tweets = searchResult.getTweets();
                for (Status tweet : tweets) {
                    sourceBuffer.add(tweet);
                }
                searchResult = searchResult.nextQuery() != null ? twitter4JClient.search(searchResult.nextQuery()) : null;
            } else {
                sourceBuffer.close();
            }
        }
    }
}
