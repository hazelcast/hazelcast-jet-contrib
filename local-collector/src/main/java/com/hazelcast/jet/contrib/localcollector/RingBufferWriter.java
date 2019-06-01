package com.hazelcast.jet.contrib.localcollector;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;

final class RingBufferWriter<T> {
    private static final int RING_BUFFER_ADD_MAX_BATCH_SIZE = 1_000;

    private final Ringbuffer<T> ringbuffer;
    private final ArrayList<T> buffer;

    RingBufferWriter(Ringbuffer<T> ringbuffer) {
        this.ringbuffer = ringbuffer;
        this.buffer = new ArrayList<>();
    }

    void receive(T item) {
        buffer.add(item);
        if (buffer.size() == RING_BUFFER_ADD_MAX_BATCH_SIZE) {
            writeAndFlush();
        }
    }

    void writeAndFlush() {
        if (buffer.isEmpty()) {
            return;
        }
        ICompletableFuture<Long> f = ringbuffer.addAllAsync(buffer, OVERWRITE);
        try {
            f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        } catch (ExecutionException e) {
            throw ExceptionUtil.rethrow(e);
        }
        buffer.clear();
    }
}
