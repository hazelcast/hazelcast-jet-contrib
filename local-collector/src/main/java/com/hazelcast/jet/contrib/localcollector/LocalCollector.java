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

package com.hazelcast.jet.contrib.localcollector;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.util.StringUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.lang.Math.min;

/**
 * Convenience for simple consumption of Pipeline results.
 *
 * Use {@link #createNew(JetInstance)} or {@link #reconnect(JetInstance)} to create a new collector.
 *
 * You have to use {@link LocalCollectorBuilder#consumer(Consumer)} or
 * {@link LocalCollectorBuilder#consumer(BiConsumer)} to register a function to receive items produced by pipeline.
 *
 * Use {@link #asSink()} to connect collector with your {@link com.hazelcast.jet.pipeline.Pipeline}
 *
 * Internally it uses a ringbuffer to store pipeline results and then it polls the ringbuffer to deliver results to
 * a registered consumer. The ringbuffer has a fixed capacity of 10,000 elements. This implies this collector is not
 * suitable for jobs which produce results in a very high-rate, think of millions of items per seconds.
 *
 * Most of jobs tend to process huge amount of input data, but they aggregate them in some way so the rate of results is
 * much lower then rate of incoming data.
 *
 *
 * @param <T> type of items generated
 */
public final class LocalCollector<T> {
    private static final long RECONNECT_FROM_LATEST = -1;
    private static final long RECONNECT_FROM_EARLIEST = -2;
    private static final int MAX_SHIFT_WHEN_CATCHING_UP = 16;
    private static final int RING_BUFFER_READ_MAX_BATCH_SIZE = 1_000;
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final String name;
    private final Ringbuffer<T> ringbuffer;
    private final boolean allowLostItems;
    private final BiConsumer<Long, T> consumer;
    private final Consumer<Throwable> exceptionConsumer;
    private volatile boolean stopped;

    private LocalCollector(JetInstance jetInstance, String name, BiConsumer<Long, T> consumer,
                           Consumer<Throwable> exceptionConsumer, long offset, boolean allowLostItems) {
        this.name = name;
        this.consumer = consumer;
        this.exceptionConsumer = exceptionConsumer;
        this.allowLostItems = allowLostItems;
        HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();
        ringbuffer = hazelcastInstance.getRingbuffer(name);
        boolean reconnecting = false;
        if (offset == RECONNECT_FROM_LATEST) {
            offset = ringbuffer.tailSequence() + 1;
            reconnecting = true;
        } else if (offset == RECONNECT_FROM_EARLIEST) {
            offset = ringbuffer.headSequence();
            reconnecting = true;
        }
        registerCallback(offset, 0, reconnecting);
    }

    /**
     * Create new local collector builder.
     *
     *
     * @param jetInstance jet instance this collector is connected to
     * @param <T> type of items to collect
     * @return builder to create a new collector
     */
    public static <T> LocalCollectorBuilder<T> createNew(JetInstance jetInstance) {
        return new LocalCollectorBuilder<>(jetInstance, 0);
    }

    /**
     * Create a local collector builder which connect to an already existing collector.
     *
     * @param jetInstance je instance this collector is connected to
     * @param <T> type of items to collect
     * @return builder to create a collector
     */
    public static <T> ReconnectionBuilder<T> reconnect(JetInstance jetInstance) {
        return new ReconnectionBuilder<>(jetInstance);
    }

    /**
     * Return a sink to connect collector with a {@link com.hazelcast.jet.pipeline.Pipeline}
     *
     * @return sink to connect to <code>Pipeline</code>
     */
    public Sink<T> asSink() {
        String localName = name;
        return SinkBuilder.sinkBuilder(localName, c -> {
            JetInstance jetInstance = c.jetInstance();
            HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();
            Ringbuffer<T> ringbuffer = hazelcastInstance.getRingbuffer(localName);
            return new RingBufferWriter<>(ringbuffer);
        }).<T>receiveFn(RingBufferWriter::receive)
                .flushFn(RingBufferWriter::writeAndFlush)
                .build();
    }

    /**
     * Get name of the collector. You can use the name when reconnecting to a collector
     * with {@link #reconnect(JetInstance)}
     *
     * @return name of this collector
     */
    public String getName() {
        return name;
    }

    /**
     * Stop the current collector. It does not stop the connected job, but it will cease delivering results
     * to your consumers.
     *
     */
    public void stop() {
        stopped = true;
    }

    /**
     * Destroy collector. It will also destroy the backing ringbuffer. You should only call this when your job is over.
     * When you call this method while your pipeline job is still running then results are undefined.
     *
     * You probably want to call {@link #stop()} instead
     *
     */
    public void destroy() {
        stopped = true;
        ringbuffer.destroy();
    }

    /**
     * Indicates whether the collector is active.
     * A collector is active unless it has been stopped or destroyed.
     *
     * @return <code>true</code> when the collector is active, otherwise it returns <code>false</code>
     */
    public boolean isActive() {
        return stopped;
    }

    private void registerCallback(long startSequence, long shift, boolean reconnecting) {
        ICompletableFuture<ReadResultSet<T>> f = ringbuffer.readManyAsync(startSequence, 1,
                RING_BUFFER_READ_MAX_BATCH_SIZE, null);
        f.andThen(new RingbufferReadCallback(startSequence, reconnecting, shift));
    }

    /**
     * Builder for reconnecting to already existing local collector
     *
     * @param <T> type of items to collect
     */
    public static final class ReconnectionBuilder<T> {
        private final JetInstance jetInstance;

        private ReconnectionBuilder(JetInstance jetInstance) {
            this.jetInstance = jetInstance;
        }

        /**
         * Reconnect to already existing collector, starting from the last produced item.
         * It will skip items produced before the very latest one even when they are still available in a backing
         * ringbuffer.
         *
         * @return builder instance
         */
        public LocalCollectorBuilder<T> fromLatest() {
            return new LocalCollectorBuilder<>(jetInstance, RECONNECT_FROM_LATEST);
        }

        /**
         * Reconnect to already existing collector, starting from the earliest available item.
         * Unlike {@link #fromLatest()} it will try to deliver all items still available in the backing
         * ringbuffer.
         *
         * @return builder instance
         */
        public LocalCollectorBuilder<T> fromEarliest() {
            return new LocalCollectorBuilder<>(jetInstance, RECONNECT_FROM_EARLIEST);
        }

        /**
         * Reconnect to already existing collector, starting from a given sequence ID.
         * This is useful when combined with {@link LocalCollectorBuilder#consumer(BiConsumer)} when you consumer
         * is tracking sequence ID of received items.
         *
         * The collector will fail when specified sequence is no longer available. You can suppress this behavior
         * by calling {@link LocalCollectorBuilder#skipLostItems()}
         *
         * @param sequenceId sequenceId to reconnect to.
         * @return builder instance
         */
        public LocalCollectorBuilder<T> fromSequence(long sequenceId) {
            if (sequenceId < 0) {
                throw new IllegalArgumentException("SequenceId cannot be negative. Sequence was set to " + sequenceId);
            }
            return new LocalCollectorBuilder<>(jetInstance, sequenceId);
        }
    }

    /**
     * Builder to configure local collector parameters
     *
     * @param <T>
     */
    public static final class LocalCollectorBuilder<T> {
        private final JetInstance jetInstance;
        private final long sequenceId;
        private String name;
        private BiConsumer<Long, T> consumer;
        private Consumer<Throwable> exceptionConsumer;
        private boolean nameSetExplicitly;
        private boolean allowLostItems;

        private LocalCollectorBuilder(JetInstance jetInstance, long sequenceId) {
            this.jetInstance = jetInstance;
            this.name = "LocalCollector-" + COUNTER.getAndIncrement();
            this.sequenceId = sequenceId;
        }

        /**
         * Explicit name of the collector. This is useful when you are planning to reconnect to the same collector via
         * {@link #reconnect(JetInstance)}
         *
         * @param name name of the collector
         * @return builder instance
         */
        public LocalCollectorBuilder<T> name(String name) {
            if (StringUtil.isNullOrEmpty(name)) {
                throw new IllegalArgumentException("Name cannot be empty");
            }
            this.name = name;
            this.nameSetExplicitly = true;
            return this;
        }

        /**
         * Collector skip missing items instead of propagating an error to {@link #exceptionConsumer(Consumer)}
         *
         * @return builder instance
         */
        public LocalCollectorBuilder<T> skipLostItems() {
            this.allowLostItems = true;
            return this;
        }


        /**
         * Register a simple consumer which will receive items produced by a connected pipeline.
         *
         * The consumer must not block calling thread. If you need to perform a blocking operation
         * then the consumer must offload this operation to another thread.
         *
         * @param consumer consumer to receive items
         * @return builder instance
         */
        public LocalCollectorBuilder<T> consumer(Consumer<T> consumer) {
            if (consumer == null) {
                throw new IllegalArgumentException("Consumer cannot be null");
            }
            this.consumer((offset, item) -> consumer.accept(item));
            return this;
        }

        /**
         * Register a consumer which will receive items produced by a connected pipeline.
         * Unlike {@link #consumer(Consumer)} the consumer also receive a sequence ID which can
         * be later used when reconnecting to the same collector via {@link ReconnectionBuilder#fromSequence(long)}
         *
         * @param consumer consumer to receive items and sequence ID
         * @return builder instance
         */
        public LocalCollectorBuilder<T> consumer(BiConsumer<Long, T> consumer) {
            if (this.consumer != null) {
                throw new IllegalStateException("Consumer is already set to " + this.consumer);
            }
            if (consumer == null) {
                throw new IllegalArgumentException("Consumer cannot be null");
            }
            this.consumer = consumer;
            return this;
        }

        /**
         * Register a consumer to receive exception.
         *
         * @param exceptionConsumer consumer to receive errors
         * @return builder instance
         */
        public LocalCollectorBuilder<T> exceptionConsumer(Consumer<Throwable> exceptionConsumer) {
            this.exceptionConsumer = exceptionConsumer;
            return this;
        }

        /**
         * Start collector.
         *
         * @return new instance of a active collector.
         */
        public LocalCollector<T> start() {
            if (consumer == null) {
                throw new IllegalStateException("Consumer cannot be null");
            }
            if (sequenceId != 0 && !nameSetExplicitly) {
                throw new IllegalStateException("When reconnecting to an existing sink "
                        + "then name has to be set explicitly");
            }
            return new LocalCollector<>(jetInstance, name, consumer, exceptionConsumer, sequenceId, allowLostItems);
        }
    }

    private class RingbufferReadCallback implements ExecutionCallback<ReadResultSet<T>> {
        private final long startSequence;
        private final boolean reconnecting;
        private final long shift;

        RingbufferReadCallback(long startSequence, boolean reconnecting, long shift) {
            this.startSequence = startSequence;
            this.reconnecting = reconnecting;
            this.shift = shift;
        }

        @Override
        public void onResponse(ReadResultSet<T> response) {
            int size = response.size();
            long currentSequence = startSequence;
            for (int i = 0; i < size; i++) {
                T item = response.get(i);
                if (stopped) {
                    return;
                }
                consumer.accept(currentSequence, item);
                currentSequence++;
            }
            if (stopped) {
                ringbuffer.destroy();
                return;
            }
            long nextSequenceToReadFrom = startSequence + size;
            registerCallback(nextSequenceToReadFrom, 0, false);
        }

        @Override
        public void onFailure(Throwable t) {
            if (stopped) {
                return;
            }
            if (allowLostItems || reconnecting) {
                t = unwrapException(t);
                if (t instanceof StaleSequenceException) {
                    catchUp(reconnecting);
                    return;
                }
            }

            if (exceptionConsumer != null) {
                exceptionConsumer.accept(t);
            }
            stopped = true;
        }

        private void catchUp(boolean reconnecting) {
            long startSequence = startingSequence();
            long nextShift = min(shift + 1, MAX_SHIFT_WHEN_CATCHING_UP);
            registerCallback(startSequence, nextShift, reconnecting);
        }

        private long startingSequence() {
            long startSequence = ringbuffer.headSequence();
            if (shift == 0) {
                return startSequence;
            }

            startSequence += (1 << shift);
            long currentTail = ringbuffer.tailSequence();
            startSequence = min(startSequence, currentTail);
            return startSequence;
        }

        private Throwable unwrapException(Throwable t) {
            if (t instanceof ExecutionException) {
                t = t.getCause();
            }
            return t;
        }
    }
}
