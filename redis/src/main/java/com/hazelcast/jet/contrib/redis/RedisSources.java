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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.ScoredValueStreamingChannel;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.EventTimeMapper.NO_NATIVE_TIME;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;
import static java.util.Collections.singletonMap;

/**
 * Contains factory methods for creating Redis sources.
 */
public final class RedisSources {

    private RedisSources() {
    }

    /**
     * Creates a {@link BatchSource} which retrieves all key-value pairs from a
     * Redis Hash, applies a mapping function on them and emits the resulting
     * data items as they become available. Assumes all keys and values are
     * {@link String}s. The returned data items are raw key-value pairs in form
     * of {@link java.util.Map.Entry}s. The batch ends when all elements have
     * been
     * received.
     *
     * @param name name of the source being created
     * @param uri  URI of the Redis server
     * @param hash identifier of the Redis Hash being used
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static BatchSource<Map.Entry<String, String>> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String hash
    ) {
        return hash(name, uri, hash, StringCodec::new, FunctionEx.identity());
    }

    /**
     * Creates a {@link BatchSource} which retrieves all key-value pairs from a
     * Redis Hash, applies a mapping function on them and emits the resulting
     * data items as they become available. Assumes all keys and values are
     * {@link String}s. The batch ends when all elements have been received.
     *
     * @param name  name of the source being created
     * @param uri   URI of the Redis server
     * @param hash  identifier of the Redis Hash being used
     * @param mapFn mapping function which transform key-value pairs into the
     *              desired output data item
     * @param <T>   type of the data items returned by the source
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> BatchSource<T> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String hash,
            @Nonnull FunctionEx<Map.Entry<String, String>, T> mapFn
    ) {
        return hash(name, uri, hash, StringCodec::new, mapFn);
    }

    /**
     * Creates a {@link BatchSource} which retrieves all key-value pairs from a
     * Redis Hash, applies a mapping function on them and emits the resulting
     * data items as they become available. The batch ends when all elements
     * have been received.
     * <p>
     * Here is an example pipeline which reads in all entries from such a Hash
     * and writes them out to a {@link com.hazelcast.map.IMap}.
     * <pre>{@code
     *     RedisURI uri = RedisURI.create("redis://localhost/");
     *     Pipeline.create()
     *          .readFrom(RedisSources.hash("source", uri, "hash",
     *                          StringCodec::new, FunctionEx.identity()))
     *          .writeTo(Sinks.map("map"));
     * }</pre>
     *
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param codecFn supplier of {@link RedisCodec} instances, used in turn for
     *                serializing/deserializing keys and values
     * @param hash    identifier of the Redis Hash being used
     * @param mapFn   mapping function which transform key-value pairs into the
     *                desired output data item
     * @param <K>     type of the hash identifiers and also the type of hash
     *                keys
     * @param <V>     type of hash values
     * @param <T>     type of the data items returned by the source
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <K, V, T> BatchSource<T> hash(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull K hash,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull FunctionEx<Map.Entry<K, V>, T> mapFn
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(hash, "hash");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(mapFn, "mapFn");

        Util.checkSerializable(codecFn, "codecFn");
        Util.checkSerializable(mapFn, "mapFn");

        return SourceBuilder
                .batch(name, context -> new HashContext<>(uri, hash, codecFn, mapFn))
                .<T>fillBufferFn(HashContext::fillBuffer)
                .destroyFn(HashContext::close)
                .build();
    }

    /**
     * Creates a {@link BatchSource} which queries a Redis Sorted Set for a
     * range of elements having their scores between the two limit values
     * provided (from & to). The returned elements will be emitted as they
     * arrive, the batch ends when all elements have been received. All keys and
     * values are assumed to be {@link String}s.
     *
     * @param name name of the source being created
     * @param uri  URI of the Redis server
     * @param key  identifier of the Redis Sorted Set
     * @param from start of the score range we are interested in (INCLUSIVE)
     * @param to   end of the score range we are interested in (INCLUSIVE)
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static BatchSource<ScoredValue<String>> sortedSet(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String key,
            long from,
            long to
    ) {
        return sortedSet(name, uri, StringCodec::new, key, from, to);
    }

    /**
     * Creates a {@link BatchSource} which queries a Redis Sorted Set for a
     * range of elements having their scores between the two limit values
     * provided (from & to). The returned elements will be emitted as they
     * arrive, the batch ends when all elements have been received.
     * <p>
     * Here's an example which reads a range from a Sorted Set, maps the items
     * to strings and drains them to some sink.
     * <pre>{@code
     *      RedisURI uri = RedisURI.create("redis://localhost/");
     *      Pipeline.create()
     *          .readFrom(RedisSources.sortedSet("source", uri, "sortedSet",
     *                          StringCodec::new, 10d, 90d))
     *          .map(sv -> (int) sv.getScore() + ":" + sv.getValue())
     *          .writeTo(sink);
     * }</pre>
     *
     * @param name    name of the source being created
     * @param uri     URI of the Redis server
     * @param codecFn supplier of {@link RedisCodec} instances, used in turn for
     *                serializing/deserializing keys and values
     * @param key     identifier of the Redis Sorted Set
     * @param from    start of the score range we are interested in (INCLUSIVE)
     * @param to      end of the score range we are interested in (INCLUSIVE)
     * @param <K>     type of the sorted set identifier
     * @param <V>     type of the values stored in the sorted set
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <K, V> BatchSource<ScoredValue<V>> sortedSet(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull K key,
            long from,
            long to
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(key, "key");

        return SourceBuilder.batch(name, ctx -> new SortedSetContext<>(uri, codecFn.get(), key, from, to))
                .<ScoredValue<V>>fillBufferFn(SortedSetContext::fillBuffer)
                .destroyFn(SortedSetContext::close)
                .build();
    }

    /**
     * Creates a {@link StreamSource} which reads all items from multiple Redis
     * Streams (starting from given indexes) and emits them as they arrive.
     * Assumes all keys and values are {@code String}s and assumes a projection
     * function which just emits message bodies from stream elements received.
     * Works with a single stream.
     *
     * @param name   name of the source being created
     * @param uri    URI of the Redis server
     * @param stream identifier of stream being used
     * @param offset start offset of the stream from which data should be
     *               requested
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<Map<String, String>> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull String stream,
            @Nonnull String offset
    ) {
        return stream(name, uri, singletonMap(stream, offset), StringCodec::new, StreamMessage::getBody);
    }

    /**
     * Creates a {@link StreamSource} which reads all items from multiple Redis
     * Streams (starting from given indexes) and emits them as they arrive.
     * Assumes all keys and values are {@code String}s and assumes a projection
     * function which just emits message bodies from stream elements received.
     *
     * @param name          name of the source being created
     * @param uri           URI of the Redis server
     * @param streamOffsets map keyed by stream identifiers, containing offset
     *                      values from which to start element retrieval of each
     *                      stream
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static StreamSource<Map<String, String>> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull Map<String, String> streamOffsets
    ) {
        return stream(name, uri, streamOffsets, StringCodec::new, StreamMessage::getBody);
    }

    /**
     * Creates a {@link StreamSource} which reads all items from multiple Redis
     * Streams (starting from given indexes) and emits them as they arrive.
     * Assumes all keys and values are {@code String}s.
     *
     * @param <T>           type of data coming out of the stream, the result of
     *                      applying the projection function over {@link
     *                      StreamMessage} instances
     * @param name          name of the source being created
     * @param uri           URI of the Redis server
     * @param streamOffsets map keyed by stream identifiers, containing offset
     *                      values from which to start element retrieval of each
     *                      stream
     * @param projectionFn  built in mapping function of the source which can be
     *                      used to map {@link StreamMessage} instances received
     *                      from Redis to an arbitrary type of output; this
     *                      could be done by an external mapping function in the
     *                      pipeline too, but it's included for convenience
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> StreamSource<T> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull Map<String, String> streamOffsets,
            @Nonnull FunctionEx<? super StreamMessage<String, String>, ? extends T> projectionFn
    ) {
        return stream(name, uri, streamOffsets, StringCodec::new, projectionFn);
    }

    /**
     * Creates a {@link StreamSource} which reads all items from multiple Redis
     * Streams (starting from given indexes) and emits them as they arrive.
     * <p>
     * Here is an example which reads all elements from two different Redis
     * Streams, maps the objects received to a stream specific ID and drains
     * the results out to some generic sink.
     * <pre>{@code
     * Map<String, String> streamOffsets = new HashMap<>();
     * streamOffsets.put("streamA", "0");
     * streamOffsets.put("streamB", "0");
     *
     * RedisURI uri = RedisURI.create("redis://localhost/");
     *
     * Pipeline.create()
     *     .readFrom(RedisSources.stream("source", uri, streamOffsets,
     *                      StringCodec::new,
     *                      mes -> mes.getStream() + " - " + mes.getId()))
     *     .withoutTimestamps()
     *     .writeTo(sink);
     * }</pre>
     *
     * @param <K>           type of the stream identifier and of fields (keys of
     *                      the Redis Stream entry's body map)
     * @param <V>           type of the field values in the message body
     * @param <T>           type of data coming out of the stream, the result of
     *                      applying the projection function over {@link
     *                      StreamMessage} instances
     * @param name          name of the source being created
     * @param uri           URI of the Redis server
     * @param streamOffsets map keyed by stream identifiers, containing offset
     *                      values from which to start element retrieval of each
     *                      stream
     * @param codecFn       supplier of {@link RedisCodec} instances, used in
     *                      turn for serializing/deserializing keys and values
     * @param projectionFn  built in mapping function of the source which can be
     *                      used to map {@link StreamMessage} instances received
     *                      from Redis to an arbitrary type of output; this
     *                      could be done by an external mapping function in the
     *                      pipeline too, but it's included for convenience
     * @return source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <K, V, T> StreamSource<T> stream(
            @Nonnull String name,
            @Nonnull RedisURI uri,
            @Nonnull Map<K, String> streamOffsets,
            @Nonnull SupplierEx<RedisCodec<K, V>> codecFn,
            @Nonnull FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
    ) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(streamOffsets, "streamOffsets");
        Objects.requireNonNull(codecFn, "codecFn");
        Objects.requireNonNull(projectionFn, "projectionFn");

        Util.checkSerializable(codecFn, "codecFn");
        Util.checkSerializable(projectionFn, "projectionFn");

        return streamFromProcessorWithWatermarks(name, false,
                w -> StreamRedisP.streamRedisP(uri, streamOffsets, w, codecFn, projectionFn));
    }

    private static class HashContext<K, V, T> implements KeyValueStreamingChannel<K, V> {

        private static final int NO_OF_ITEMS_TO_FETCH_AT_ONCE = 100;

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final FunctionEx<Map.Entry<K, V>, T> mapFn;
        private final BlockingQueue<Map.Entry<K, V>> queue = new ArrayBlockingQueue<>(NO_OF_ITEMS_TO_FETCH_AT_ONCE);
        private final RedisFuture<Long> commandFuture;
        private final List<Map.Entry<K, V>> batchHolder = new ArrayList<>(NO_OF_ITEMS_TO_FETCH_AT_ONCE);

        private volatile InterruptedException exception;

        HashContext(
                RedisURI uri,
                K hash,
                SupplierEx<RedisCodec<K, V>> codecSupplier,
                FunctionEx<Map.Entry<K, V>, T> mapFn
        ) {
            this.client = RedisClient.create(uri);
            this.connection = client.connect(codecSupplier.get());

            this.mapFn = mapFn;

            RedisAsyncCommands<K, V> commands = connection.async();
            this.commandFuture = commands.hgetall(this, hash);
        }

        void close() {
            connection.close();
            client.shutdown();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<T> buffer) throws InterruptedException {
            if (exception != null) { //something went wrong on the Redis client thread
                throw exception;
            }

            int itemsFetched = queue.drainTo(batchHolder, NO_OF_ITEMS_TO_FETCH_AT_ONCE);
            if (itemsFetched <= 0) {
                if (commandFuture.isDone()) {
                    buffer.close();
                }
            } else {
                batchHolder.stream()
                           .map(mapFn)
                           .forEach(buffer::add);
                batchHolder.clear();
            }
        }

        @Override
        public void onKeyValue(K key, V value) {
            Map.Entry<K, V> mapEntry = new AbstractMap.SimpleImmutableEntry<>(key, value);
            while (true) {
                try {
                    queue.put(mapEntry);
                    return;
                } catch (InterruptedException e) {
                    exception = e;
                }
            }
        }
    }

    private static final class SortedSetContext<K, V> implements ScoredValueStreamingChannel<V> {

        private static final int NO_OF_ITEMS_TO_FETCH_AT_ONCE = 100;
        private static final Duration POLL_DURATION = Duration.ofMillis(100);

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final BlockingQueue<ScoredValue<V>> queue = new ArrayBlockingQueue<>(NO_OF_ITEMS_TO_FETCH_AT_ONCE);
        private final RedisFuture<Long> commandFuture;

        private volatile InterruptedException exception;

        SortedSetContext(RedisURI uri, RedisCodec<K, V> codec, K key, long start, long stop) {
            client = RedisClient.create(uri);
            connection = client.connect(codec);

            RedisAsyncCommands<K, V> commands = connection.async();
            commandFuture = commands.zrangebyscoreWithScores(this, key, Range.create(start, stop));
        }


        void close() {
            connection.close();
            client.shutdown();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<ScoredValue<V>> buffer) throws InterruptedException {
            if (exception != null) { //something went wrong on the Redis client thread
                throw exception;
            }

            for (int i = 0; i < NO_OF_ITEMS_TO_FETCH_AT_ONCE; i++) {
                ScoredValue<V> item = queue.poll(POLL_DURATION.toMillis(), TimeUnit.MILLISECONDS);
                if (item == null) {
                    if (commandFuture.isDone()) {
                        buffer.close();
                    }
                    return;
                } else {
                    buffer.add(item);
                }
            }

        }

        @Override
        public void onValue(ScoredValue<V> value) {
            while (true) {
                try {
                    queue.put(value);
                    return;
                } catch (InterruptedException e) {
                    exception = e;
                }
            }
        }
    }

    private static class StreamRedisP<K, V, T> extends AbstractProcessor {

        private static final Duration POLL_DURATION = Duration.ofMillis(50);
        private static final int BATCH_COUNT = 1_000;

        private final RedisURI uri;
        private final Map<K, String> streamOffsets;
        private final SupplierEx<RedisCodec<K, V>> codecFn;
        private final EventTimeMapper<? super T> eventTimeMapper;
        private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

        private RedisClient redisClient;
        private StatefulRedisConnection<K, V> connection;
        private XReadArgs xReadArgs;

        private Traverser<Object> traverser = Traversers.empty();
        private Traverser<Map.Entry<BroadcastKey<K>, Object[]>> snapshotTraverser;

        StreamRedisP(
                RedisURI uri,
                Map<K, String> streamOffsets,
                EventTimePolicy<? super T> eventTimePolicy,
                SupplierEx<RedisCodec<K, V>> codecFn,
                FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
        ) {

            this.uri = uri;
            this.streamOffsets = streamOffsets;
            this.codecFn = codecFn;
            this.projectionFn = projectionFn;

            this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        }

        static <K, V, T> ProcessorMetaSupplier streamRedisP(
                RedisURI uri,
                Map<K, String> streamOffsets,
                EventTimePolicy<? super T> eventTimePolicy,
                SupplierEx<RedisCodec<K, V>> codecFn,
                FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
        ) {
            return new MetaSupplier<>(uri, streamOffsets, eventTimePolicy, codecFn, projectionFn);
        }

        @Override
        protected void init(Context context) {
            redisClient = RedisClient.create(uri);
            connection = redisClient.connect(codecFn.get());

            xReadArgs = XReadArgs.Builder.block(POLL_DURATION).count(BATCH_COUNT);
            eventTimeMapper.addPartitions(streamOffsets.size());
        }

        public boolean complete() {
            if (!emitFromTraverser(traverser)) {
                return false;
            }

            RedisCommands<K, V> sync = connection.sync();
            @SuppressWarnings("unchecked")
            XReadArgs.StreamOffset<K>[] streamOffsetArray = streamOffsets
                    .entrySet()
                    .stream()
                    .map(entry -> XReadArgs.StreamOffset.from(entry.getKey(), entry.getValue()))
                    .toArray(XReadArgs.StreamOffset[]::new);
            List<StreamMessage<K, V>> messages = sync.xread(xReadArgs, streamOffsetArray);

            traverser = isEmpty(messages) ? eventTimeMapper.flatMapIdle() :
                    traverseIterable(messages)
                            .flatMap(message -> {
                                streamOffsets.put(message.getStream(), message.getId());
                                T projectedMessage = projectionFn.apply(message);
                                if (projectedMessage == null) {
                                    return Traversers.empty();
                                }
                                return eventTimeMapper.flatMapEvent(projectedMessage, 0, NO_NATIVE_TIME);
                            });

            emitFromTraverser(traverser);

            return false;
        }

        @Override
        public void close() {
            if (connection != null) {
                connection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
        }

        @Override
        public boolean saveToSnapshot() {
            if (!emitFromTraverser(traverser)) {
                return false;
            }

            if (snapshotTraverser == null) {
                Stream<Map.Entry<BroadcastKey<K>, Object[]>> snapshotStream = streamOffsets
                        .entrySet()
                        .stream()
                        .map(entry -> {
                            long watermark = eventTimeMapper.getWatermark(0);
                            return entry(broadcastKey(entry.getKey()), new Object[] {entry.getValue(), watermark});
                        });
                snapshotTraverser = traverseStream(snapshotStream)
                        .onFirstNull(() -> {
                            snapshotTraverser = null;
                            if (getLogger().isFineEnabled()) {
                                getLogger().fine("Finished saving snapshot. Saved offsets: " + streamOffsets);
                            }
                        });
            }

            return emitFromTraverserToSnapshot(snapshotTraverser);
        }

        @Override
        public void restoreFromSnapshot(Object key, Object value) {
            K stream = ((BroadcastKey<K>) key).key();
            Object[] objects = (Object[]) value;
            String offset = (String) objects[0];
            long watermark = (long) objects[1];

            String oldOffset = streamOffsets.get(stream);
            if (oldOffset == null) {
                getLogger().warning("Offset for stream '" + stream
                        + "' is present in snapshot, but the stream is not supposed to be read");
                return;
            }
            streamOffsets.put(stream, offset);
            eventTimeMapper.restoreWatermark(0, watermark);
        }

        @Override
        public boolean finishSnapshotRestore() {
            if (getLogger().isFineEnabled()) {
                getLogger().fine("Finished restoring snapshot. Restored offsets: " + streamOffsets);
            }
            return true;
        }

        static class MetaSupplier<K, V, T> implements ProcessorMetaSupplier {

            private final RedisURI uri;
            private final Map<K, String> streamOffsets;
            private final EventTimePolicy<? super T> eventTimePolicy;
            private final SupplierEx<RedisCodec<K, V>> codecFn;
            private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

            MetaSupplier(
                    RedisURI uri,
                    Map<K, String> streamOffsets,
                    EventTimePolicy<? super T> eventTimePolicy,
                    SupplierEx<RedisCodec<K, V>> codecFn,
                    FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
            ) {
                this.uri = uri;
                this.streamOffsets = streamOffsets;
                this.eventTimePolicy = eventTimePolicy;
                this.codecFn = codecFn;
                this.projectionFn = projectionFn;
            }

            @Override
            public int preferredLocalParallelism() {
                return 2;
            }

            @Override
            @Nonnull
            public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
                int size = addresses.size();
                Object[] array = streamOffsets.keySet().toArray();
                Arrays.sort(array);

                Map<Address, Map<K, String>> addressAssignment = new HashMap<>();
                addresses.forEach(address -> addressAssignment.put(address, new HashMap<>()));
                for (int i = 0; i < array.length; i++) {
                    K stream = (K) array[i];
                    int addressIndex = i % size;
                    Address address = addresses.get(addressIndex);
                    addressAssignment.get(address).put(stream, streamOffsets.get(stream));
                }
                return address -> new ProcSupplier<>(uri, addressAssignment.get(address),
                        eventTimePolicy, codecFn, projectionFn);
            }
        }

        static class ProcSupplier<K, V, T> implements ProcessorSupplier {

            private final RedisURI uri;
            private final Map<K, String> streamOffsets;
            private final EventTimePolicy<? super T> eventTimePolicy;
            private final SupplierEx<RedisCodec<K, V>> codecFn;
            private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

            ProcSupplier(
                    RedisURI uri,
                    Map<K, String> streamOffsets,
                    EventTimePolicy<? super T> eventTimePolicy,
                    SupplierEx<RedisCodec<K, V>> codecFn,
                    FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
            ) {
                this.uri = uri;
                this.streamOffsets = streamOffsets;
                this.eventTimePolicy = eventTimePolicy;
                this.codecFn = codecFn;
                this.projectionFn = projectionFn;
            }

            @Override
            public Collection<? extends Processor> get(int count) {
                Object[] array = streamOffsets.keySet().toArray();
                Arrays.sort(array);

                Map<Integer, Map<K, String>> assignment = new HashMap<>();
                IntStream.range(0, count).forEach(address -> assignment.put(address, new HashMap<>()));
                for (int i = 0; i < array.length; i++) {
                    K stream = (K) array[i];
                    int addressIndex = i % count;
                    assignment.get(addressIndex).put(stream, streamOffsets.get(stream));
                }
                ArrayList<Processor> list = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    Map<K, String> assignedStreamOffsets = assignment.get(i);
                    if (assignedStreamOffsets.isEmpty()) {
                        list.add(Processors.noopP().get());
                    } else {
                        list.add(new StreamRedisP<>(uri, assignedStreamOffsets, eventTimePolicy,
                                codecFn, projectionFn));
                    }
                }
                return list;
            }
        }
    }

}
