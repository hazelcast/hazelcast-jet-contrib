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

package com.hazelcast.jet.contrib.redis;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;

/**
 * Contains factory methods for creating Redis sinks.
 */
public final class RedisSinks {

    private RedisSinks() {
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Hash. Assumes that all keys and values are {@link String}s and that
     * incoming data items are raw {@link java.util.Map.Entry}s.
     *
     * @param name name of the source being created
     * @param uri  URI of the Redis server
     * @param hash identifier of the Redis Hash being used
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static Sink<Map.Entry<String, String>> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String hash
    ) {
        return hash(name, uri, hash, StringCodec::new, Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Hash. Assumes that all keys and values are {@link String}s.
     *
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param hash    identifier of the Redis Hash being used
     * @param keyFn   function that specifies how to extract the hash key from
     *                incoming data items
     * @param valueFn function that specifies how to extract the hash value from
     *                incoming data items
     * @param <T>     type of incoming data items
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <T> Sink<T> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String hash,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, String> valueFn
    ) {
        return hash(name, uri, hash, StringCodec::new, keyFn, valueFn);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Hash.
     * <p>
     * Here is an example which reads map entries from a {@link
     * com.hazelcast.map.IMap} and writes them out into a Redis Hash.
     * <pre>{@code
     *  RedisURI uri = RedisURI.create("redis://localhost/");
     *  Pipeline.create()
     *      .readFrom(Sources.map(map))
     *      .writeTo(RedisSinks.hash("sink", uri, "hash", StringCodec::new,
     *                      Map.Entry::getKey, Map.Entry::getValue));
     * }</pre>
     *
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param codecFn supplier of {@link RedisCodec} instances, used in turn for
     *                serializing/deserializing keys and values
     * @param hash    identifier of the Redis Hash being used
     * @param keyFn   function that specifies how to extract the hash key from
     *                incoming data items
     * @param valueFn function that specifies how to extract the hash value from
     *                incoming data items
     * @param <K>     type of the hash identifier and type of hash keys
     * @param <V>     type of hash values
     * @param <T>     type of incoming data items
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <K, V, T> Sink<T> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull K hash,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull FunctionEx<T, K> keyFn,
            @Nonnull FunctionEx<T, V> valueFn
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(hash, "hash");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(keyFn, "keyFn");
        Objects.requireNonNull(valueFn, "valueFn");

        Util.checkSerializable(codecFn, "codecFn");
        Util.checkSerializable(keyFn, "keyFn");
        Util.checkSerializable(valueFn, "valueFn");

        return SinkBuilder
                .sinkBuilder(name, context -> new HashContext<>(uri, hash, codecFn.get(), keyFn, valueFn))
                .<T>receiveFn(HashContext::store)
                .flushFn(HashContext::flush)
                .destroyFn(HashContext::close)
                .build();
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Sorted Set. Assumes that the sorted set identifier and stored values are
     * all {@code String}s and the incoming data items are
     * {@link io.lettuce.core.ScoredValue}s.
     *
     * @param name name of the source being created
     * @param uri  URI of the Redis server
     * @param set  identifier of the Redis Sorted Set being used
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static Sink<ScoredValue<String>> sortedSet(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String set
    ) {
        return sortedSet(name, uri, set, StringCodec::new, ScoredValue::getScore, ScoredValue::getValue);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Sorted Set. Assumes that the sorted set identifier and stored values are
     * all {@code String}s.
     *
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param set     identifier of the Redis Sorted Set being used
     * @param scoreFn function that specifies how to extract the scores from
     *                incoming data items
     * @param valueFn function that specifies how to extract stored values from
     *                incoming data items
     * @param <T>     type of data items coming into the sink
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <T> Sink<T> sortedSet(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String set,
            @Nonnull FunctionEx<T, Double> scoreFn,
            @Nonnull FunctionEx<T, String> valueFn
    ) {
        return sortedSet(name, uri, set, StringCodec::new, scoreFn, valueFn);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Sorted Set.
     * <p>
     * Here is an example which reads out trades from a source and writes them
     * to a Redis Stream.
     * <pre>{@code
     * RedisURI uri = RedisURI.create("redis://localhost/");
     * Pipeline.create()
     *      .readFrom(source)
     *      .map(trade -> ScoredValue.fromNullable(trade.timestamp, trade))
     *      .writeTo(RedisSinks.sortedSet("sink", uri, "sortedSet",
     *                      StringCodec::new,
     *                      ScoredValue::getScore, ScoredValue::getValue));
     * }</pre>
     *
     * @param <K>     type of sorted set identifier
     * @param <V>     type of stored values
     * @param <T>     type of data items coming into the sink
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param set     identifier of the Redis Sorted Set being used
     * @param codecFn supplier of {@link RedisCodec} instances, used in turn for
     *                serializing/deserializing keys and values
     * @param scoreFn function that specifies how to extract the scores from
     *                incoming data items
     * @param valueFn function that specifies how to extract stored values from
     *                incoming data items
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <K, V, T> Sink<T> sortedSet(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull K set,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull FunctionEx<T, Double> scoreFn,
            @Nonnull FunctionEx<T, V> valueFn
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(set, "set");
        Objects.requireNonNull(scoreFn, "scoreFn");
        Objects.requireNonNull(valueFn, "valueFn");

        Util.checkSerializable(codecFn, "codecFn");
        Util.checkSerializable(scoreFn, "scoreFn");
        Util.checkSerializable(valueFn, "valueFn");

        return SinkBuilder.sinkBuilder(name, ctx -> new SortedSetContext<>(uri, set, codecFn.get(), scoreFn, valueFn))
                .<T>receiveFn(SortedSetContext::store)
                .flushFn(SortedSetContext::flush)
                .destroyFn(SortedSetContext::destroy)
                .build();
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Stream. Assumes all keys and values are {@code String}s. Uses a very
     * simple mapping function which translates incoming data items directly to
     * field values (via their {@code toString()} method).
     *
     * @param <T>    type of elements forming the input of this sink
     * @param name   name of the source being created
     * @param uri    URI of the Redis server
     * @param stream identifier of stream being used
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <T> Sink<T> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String stream
    ) {
        FunctionEx<T, Map<String, String>> mapFn = item -> singletonMap("", item.toString());
        return stream(name, uri, stream, StringCodec::new, mapFn);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Stream. Assumes all keys and values are {@code String}s.
     *
     * @param <T>    type of elements forming the input of this sink
     * @param name   name of the source being created
     * @param uri    URI of the Redis server
     * @param stream identifier of stream being used
     * @param mapFn  mapping function used to transform an incoming element into
     *               a Redis Stream entry's body
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <T> Sink<T> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, Map<String, String>> mapFn
    ) {
        return stream(name, uri, stream, StringCodec::new, mapFn);
    }

    /**
     * Creates a {@link Sink} which pushes data items into a specified Redis
     * Stream.
     * <p>
     * Here is an example pipeline which reads out measurements from
     * Hazelcast List and writes them to a Redis Stream.
     * <pre>{@code
     * RedisURI uri = RedisURI.create("redis://localhost/");
     * FunctionEx<T, Map<String, String>> mapFn = item -> singletonMap("", item.toString());
     * Pipeline.create()
     *     .readFrom(Sources.list(list))
     *     .writeTo(RedisSinks.stream("sink", uri, "stream", StringCodec::new, mapFn));
     * } </pre>
     * @param <T>     type of elements forming the input of this sink
     * @param <K>     type of the stream identifier and of fields (keys of the
     *                Redis Stream entry's body map)
     * @param <V>     type of the field values in the message body
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param stream  identifier of stream being used
     * @param codecFn supplier of {@link RedisCodec} instances, used in turn for
     *                serializing/deserializing keys and values
     * @param mapFn   mapping function used to transform an incoming element
     *                into a Redis Stream entry's body
     * @return sink to use in {@link com.hazelcast.jet.pipeline.Pipeline#writeTo}
     */
    @Nonnull
    public static <T, K, V> Sink<T> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull K stream,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull FunctionEx<T, Map<K, V>> mapFn
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(mapFn, "mapFn");

        Util.checkSerializable(codecFn, "codecFn");
        Util.checkSerializable(mapFn, "mapFn");

        return SinkBuilder
                .sinkBuilder(name, c -> new StreamContext<>(uri, stream, codecFn, mapFn))
                .<T>receiveFn(StreamContext::add)
                .flushFn(StreamContext::flush)
                .destroyFn(StreamContext::close)
                .build();
    }

    private static class HashContext<K, V, T> {

        private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(1);

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final RedisAsyncCommands<K, V> commands;
        private final K hash;
        private final FunctionEx<T, K> keyFn;
        private final FunctionEx<T, V> valueFn;
        private final Map<K, V> map = new HashMap<>();
        private RedisFuture<String> future;

        HashContext(RedisURI uri, K hash, RedisCodec<K, V> codec, FunctionEx<T, K> keyFn,
                    FunctionEx<T, V> valueFn) {
            this.client = RedisClient.create(uri);
            this.connection = client.connect(codec);
            this.commands = connection.async();
            this.hash = hash;
            this.keyFn = keyFn;
            this.valueFn = valueFn;
        }

        void store(T item) {
            K key = keyFn.apply(item);
            V value = valueFn.apply(item);
            map.put(key, value);
        }

        void flush() throws InterruptedException {
            //make sure previous flush is finished
            if (future != null) {
                boolean flushed = future.await(FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                if (future.getError() != null) {
                    throw new RuntimeException(future.getError());
                }
                if (!flushed) {
                    throw new RuntimeException("Flushing failed!");
                }
            }

            //initiate next flush
            future = commands.hmset(hash, map);

            //reset internal accumulator
            map.clear();
        }

        void close() {
            connection.close();
            client.shutdown();
        }
    }

    private static class SortedSetContext<K, V, T> {

        private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(10);

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final RedisAsyncCommands<K, V> commands;
        private final ArrayList<RedisFuture<Long>> futures = new ArrayList<>();
        private final K sortedSet;
        private final FunctionEx<T, Double> scoreFn;
        private final FunctionEx<T, V> valueFn;

        SortedSetContext(RedisURI uri, K sortedSet, RedisCodec<K, V> codec, FunctionEx<T, Double> scoreFn,
                         FunctionEx<T, V> valueFn) {
            this.client = RedisClient.create(uri);
            this.connection = client.connect(codec);
            this.commands = connection.async();
            this.sortedSet = sortedSet;
            this.scoreFn = scoreFn;
            this.valueFn = valueFn;
        }

        void store(T t) {
            futures.add(commands.zadd(sortedSet, scoreFn.apply(t), valueFn.apply(t)));
        }

        void flush() {
            boolean flushed = LettuceFutures.awaitAll(FLUSH_TIMEOUT, futures.toArray(new RedisFuture[0]));
            if (!flushed) {
                throw new RuntimeException("Flushing failed!");
            }
            futures.clear();
        }

        void destroy() {
            connection.close();
            client.shutdown();
        }
    }

    private static final class StreamContext<K, V, T> {

        private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(10);

        private final RedisClient redisClient;
        private final StatefulRedisConnection<K, V> connection;
        private final FunctionEx<T, Map<K, V>> mapFn;
        private final K stream;
        private final List<RedisFuture<String>> futures = new ArrayList<>();

        private StreamContext(
                RedisURI uri,
                K stream,
                SupplierEx<RedisCodec<K, V>> codecFn,
                FunctionEx<T, Map<K, V>> mapFn
        ) {
            this.stream = stream;
            this.mapFn = mapFn;

            redisClient = RedisClient.create(uri);
            connection = redisClient.connect(codecFn.get());
        }

        void add(T item) {
            RedisAsyncCommands<K, V> async = connection.async();
            Map<K, V> body = mapFn.apply(item);
            RedisFuture<String> future = async.xadd(stream, body);
            futures.add(future);
        }

        void flush() {
            boolean flushed = LettuceFutures.awaitAll(FLUSH_TIMEOUT, futures.toArray(new RedisFuture[0]));
            if (!flushed) {
                throw new RuntimeException("Flushing failed!");
            }
            futures.clear();
        }

        void close() {
            connection.close();
            redisClient.shutdown();
        }
    }
}

