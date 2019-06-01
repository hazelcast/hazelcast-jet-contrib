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
