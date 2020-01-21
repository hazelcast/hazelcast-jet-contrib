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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Objects;
import java.util.Properties;

import javax.annotation.Nonnull;


/**
 * Contains methods for creating Twitter stream sources.
 */
public final class TwitterSources {

    private TwitterSources() {
    }


    /**
     * Creates a {@link StreamSource} which reads tweets from Twitter's Streaming API for data ingestion to Jet
     * pipelines.
     * <p>
     * Example usage:
     * <pre>{@code
     * Properties credentials = loadTwitterCredentials();
     * List<String> terms = new ArrayList<String>(Arrays.asList("BTC", "ETH"));
     * StreamSource<String> streamSource =
     *              TwitterSources.stream(
     *                      credentials,
     *                      Constants.STREAM_HOST,
     *                      () -> new StatusesFilterEndpoint().trackTerms(terms)
     *              );
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<String> srcStage = p.readFrom(streamSource);
     * }</pre>
     *
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey",
     *                         "consumerSecret", "token", "tokenSecret" keys.
     * @param host             a Twitter host URL to connect.
     *                         These hosts are defined in {@link com.twitter.hbc.core.Constants}.
     * @param endpointSupplier a supplier function that supplies a Twitter StreamingEndpoint to connect to source.
     * @return a stream source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> stream(
            @Nonnull Properties credentials,
            @Nonnull String host,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return SourceBuilder.stream("twitter-stream-source",
                ctx -> new TwitterSourceContext(credentials, host, endpointSupplier))
                .fillBufferFn(TwitterSourceContext::fillBuffer)
                .destroyFn(TwitterSourceContext::close)
                .build();
    }

    /**
     * The method {@link TwitterSources#stream(Properties, String, SupplierEx)} is overloaded here.
     * This method differs from the other in the aspect of using {@link com.twitter.hbc.core.Constants#STREAM_HOST}
     * as a default Twitter Streaming API host.
     * <p>
     * Example usage:
     * <pre>{@code
     * Properties credentials = loadTwitterCredentials();
     * List<String> terms = new ArrayList<String>(Arrays.asList("BTC", "ETH"));
     * StreamSource<String> streamSource =
     *              TwitterSources.stream(
     *                      credentials,
     *                      () -> new StatusesFilterEndpoint().trackTerms(terms)
     *              );
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<String> srcStage = p.readFrom(streamSource);
     * }</pre>
     *
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey",
     *                         "consumerSecret", "token", "tokenSecret" keys.
     * @param endpointSupplier a supplier function that supplies a Twitter StreamingEndpoint to connect to source.
     * @return a stream source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> stream(@Nonnull Properties credentials,
                                              @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return stream(credentials, Constants.STREAM_HOST, endpointSupplier);
    }


    /**
     * The timestampedStream is almost same with {@link TwitterSources#stream(Properties, String, SupplierEx)}.
     * The only difference is that the timestampedStream creates a timestamped stream source while the
     * other creates without timestamps.
     *
     * <p>
     * Example usage:
     * <pre>{@code
     *
     * List<Long> userIds = new ArrayList<Long>(
     *                 Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
     * StreamSource<String> timestampedStreamSource =
     *              TwitterSources.timestampedStream(
     *                      credentials,
     *                      Constants.STREAM_HOST,
     *                      () -> new StatusesFilterEndpoint().followings(userIds),
     *              );
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<String> srcStage = p.readFrom(timestampedStreamSource);
     * }</pre>
     *
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey",
     *                         "consumerSecret", "token", "tokenSecret" keys.
     *                         These hosts are defined in {@link com.twitter.hbc.core.Constants}.
     * @param host             a Twitter host URL to connect.
     * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source.
     * @return a timestamped stream source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */

    @Nonnull
    public static StreamSource<String> timestampedStream(
            @Nonnull Properties credentials,
            @Nonnull String host,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier

    ) {
        return SourceBuilder.timestampedStream("twitter-timestamped-stream-source",
                ctx -> new TwitterSourceContext(credentials, host, endpointSupplier))
                .fillBufferFn(TwitterSourceContext::fillTimestampedBuffer)
                .destroyFn(TwitterSourceContext::close)
                .build();
    }

    /**
     * The method {@link TwitterSources#timestampedStream(Properties, String, SupplierEx)} is overloaded here.
     * This method differs from the other in the aspect of using {@link com.twitter.hbc.core.Constants#STREAM_HOST}
     * as a default Twitter Streaming API host.
     * <p>
     * Example usage:
     * <pre>{@code
     *
     * List<Long> userIds = new ArrayList<Long>(
     *                 Arrays.asList(612473L, 759251L, 1367531L, 34713362L, 51241574L, 87818409L));
     * StreamSource<String> timestampedStreamSource =
     *              TwitterSources.timestampedStream(
     *                      credentials,
     *                      () -> new StatusesFilterEndpoint().followings(userIds),
     *              );
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<String> srcStage = p.readFrom(timestampedStreamSource);
     * }</pre>
     *
     * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey",
     *                         "consumerSecret", "token", "tokenSecret" keys.
     * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source.
     * @return a timestamped stream source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<String> timestampedStream(
            @Nonnull Properties credentials,
            @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
    ) {
        return timestampedStream(credentials, Constants.STREAM_HOST, endpointSupplier);
    }


    /**
     * A source context of Twitter
     */
    private static final class TwitterSourceContext {

        private static final int QUEUE_CAPACITY = 1000;
        private static final int MAX_FILL_ELEMENTS = 250;

        private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        private final ArrayList<String> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final BasicClient client;

        /**
         * @param credentials      a Twitter OAuth1 credentials that consists "consumerKey",
         *                         "consumerSecret", "token", "tokenSecret" keys.
         * @param host             a Twitter host URL to connect.
         *                         These hosts are defined in {@link com.twitter.hbc.core.Constants}.
         * @param endpointSupplier Supplier that supplies a Twitter StreamingEndpoint to connect to source.
         */
        private TwitterSourceContext(
                @Nonnull Properties credentials,
                @Nonnull String host,
                @Nonnull SupplierEx<? extends StreamingEndpoint> endpointSupplier
        ) {
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

        private void checkTwitterCredentials(Properties credentials) {
            String consumerKey = credentials.getProperty("consumerKey");
            String consumerSecret = credentials.getProperty("consumerSecret");
            String token = credentials.getProperty("token");
            String tokenSecret = credentials.getProperty("tokenSecret");

            isMissing(consumerKey, "consumerKey");
            isMissing(consumerSecret, "consumerSecret");
            isMissing(token, "token");
            isMissing(tokenSecret, "tokenSecret");
        }

        private static void isMissing(String key, String description) {
            Objects.requireNonNull(key, description);
            if ("REPLACE_THIS".equals(key)) {
                throw new IllegalArgumentException("Twitter credentials key: " + description + " is missing!");
            }
        }
    }

}