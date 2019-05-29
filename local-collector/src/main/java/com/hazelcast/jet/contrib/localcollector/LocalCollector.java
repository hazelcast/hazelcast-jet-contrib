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
import com.hazelcast.util.UuidUtil;

import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;

public final class LocalCollector<T> {
    private static final long RECONNECT_FROM_LATEST = -1;

    private final String name;
    private final Ringbuffer<T> ringbuffer;
    private final boolean allowLostItems;
    private final BiConsumer<Long, T> consumer;
    private final Consumer<Throwable> exceptionConsumer;
    private volatile boolean stopped;

    public static <T> LocalCollectorBuilder<T> createNew(JetInstance jetInstance) {
        return new LocalCollectorBuilder<>(jetInstance, 0);
    }

    public static <T> Reconnection<T> reconnect(JetInstance jetInstance) {
        return new Reconnection<>(jetInstance);
    }

    public Sink<T> asSink() {
        String localName = name;
        return SinkBuilder.sinkBuilder(localName, c -> {
            JetInstance jetInstance = c.jetInstance();
            HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();
            return hazelcastInstance.getRingbuffer(localName);
        }).<T>receiveFn((r, i) -> r.addAsync(i, OVERWRITE))
                .build();
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        stopped = true;
        ringbuffer.destroy();
    }

    public void stop() {
        stopped = true;
    }

    public boolean isActive() {
        return stopped;
    }

    private LocalCollector(JetInstance jetInstance, String name, BiConsumer<Long, T> consumer,
                           Consumer<Throwable> exceptionConsumer, long offset, boolean allowLostItems) {
        this.name = name;
        this.consumer = consumer;
        this.exceptionConsumer = exceptionConsumer;
        this.allowLostItems = allowLostItems;
        HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();
        ringbuffer = hazelcastInstance.getRingbuffer(name);
        if (offset == RECONNECT_FROM_LATEST) {
            offset = ringbuffer.tailSequence() + 1;
        }
        registerCallback(offset, 0);
    }

    private void registerCallback(long startingSequence, long retryNo) {
        ICompletableFuture<ReadResultSet<T>> f = ringbuffer.readManyAsync(startingSequence, 1, 100, null);
        f.andThen(new ExecutionCallback<ReadResultSet<T>>() {
            @Override
            public void onResponse(ReadResultSet<T> response) {
                int size = response.size();
                long currentSequence = startingSequence;
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
                long nextSequenceToReadFrom = startingSequence + size;
                registerCallback(nextSequenceToReadFrom, 0);
            }

            @Override
            public void onFailure(Throwable t) {
                if (allowLostItems) {
                    t = unwrapException(t);
                    if (t instanceof StaleSequenceException) {
                        long headSeq = ringbuffer.headSequence();
                        if (retryNo != 0) {
                            long tail = ringbuffer.tailSequence();
                            headSeq += 1 << retryNo;
                            headSeq = Math.min(headSeq, tail);
                        }
                        long newRetry = retryNo + 1;
                        newRetry = Math.min(newRetry, 16);
                        registerCallback(headSeq, newRetry);
                        System.out.println( " ---------- STALE SEQUENCE, RETRY: " + retryNo);
                        return;
                    }
                }

                if (stopped) {
                    return;
                }

                if (exceptionConsumer != null) {
                    exceptionConsumer.accept(t);
                }
                stopped = true;
            }

            private Throwable unwrapException(Throwable t) {
                if (t instanceof ExecutionException) {
                    t = t.getCause();
                }
                return t;
            }
        });
    }

    public static final class Reconnection<T> {
        private final JetInstance jetInstance;

        private Reconnection(JetInstance jetInstance) {
            this.jetInstance = jetInstance;
        }

        public LocalCollectorBuilder<T> fromOffset(long offset) {
            return new LocalCollectorBuilder<>(jetInstance, offset);
        }

        public LocalCollectorBuilder<T> fromLatest() {
            return new LocalCollectorBuilder<>(jetInstance, RECONNECT_FROM_LATEST);
        }
    }

    public static final class LocalCollectorBuilder<T> {
        private final JetInstance jetInstance;
        private final long offset;
        private String name;
        private BiConsumer<Long, T> consumer;
        private Consumer<Throwable> exceptionConsumer;
        private boolean nameSetExplicitly;
        private boolean allowLostItems;

        private LocalCollectorBuilder(JetInstance jetInstance, long offset) {
            this.jetInstance = jetInstance;
            this.name = "LocalCollector-" + UuidUtil.newUnsecureUuidString();
            this.offset = offset;
        }

        public LocalCollectorBuilder<T> name(String name) {
            if (StringUtil.isNullOrEmpty(name)) {
                throw new IllegalArgumentException("Name cannot be empty");
            }
            this.name = name;
            this.nameSetExplicitly = true;
            return this;
        }

        public LocalCollectorBuilder<T> skipLostItems() {
            this.allowLostItems = true;
            return this;
        }

        public LocalCollectorBuilder<T> consumer(Consumer<T> consumer) {
            if (consumer == null) {
                throw new IllegalArgumentException("Consumer cannot be null");
            }
            this.consumer((offset, item) -> consumer.accept(item));
            return this;
        }

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

        public LocalCollectorBuilder<T> exceptionConsumer(Consumer<Throwable> exceptionConsumer) {
            this.exceptionConsumer = exceptionConsumer;
            return this;
        }

        public LocalCollector<T> start() {
            if (consumer == null) {
                throw new IllegalStateException("Consumer cannot be null");
            }
            if (offset != 0 && !nameSetExplicitly) {
                throw new IllegalStateException("When reconnecting to an existing sink "
                        + "then name has to be set explicitly");
            }
            return new LocalCollector<>(jetInstance, name, consumer, exceptionConsumer, offset, allowLostItems);
        }

    }
}
