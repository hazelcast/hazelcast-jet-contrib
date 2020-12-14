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

package com.hazelcast.jet.contrib.reliabletopic;

import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.topic.Message;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Contains a method for creating a reliable topic stream sources.
 */
public final class ReliableTopicSource {

    private static final int DEFAULT_QUEUE_CAP = 1024;

    private ReliableTopicSource() {
    }

    /**
     * Creates a {@link StreamSource} which reads messages from
     * from a Hazelcast Reliable Topic with the specified name.
     *
     * @param topic             topic name to read from
     * @param <T>               type of emitted item/object inside
     *                          a message.
     * @return {@link StreamSource}
     */
    public static <T> StreamSource<T> reliableTopicSource(
            @Nonnull String topic
    ) {
        return reliableTopicSource(topic, DEFAULT_QUEUE_CAP);
    }

    /**
     * Creates a {@link StreamSource} which reads messages from
     * from a Hazelcast Reliable Topic with the specified name.
     *
     * @param topic             topic name to read from
     * @param queueCapacity     specifies the capacity of the blocking
     *                          queue that is used to buffer items.
     * @param <T>               type of emitted item/object inside
     *                          a message.
     * @return {@link StreamSource}
     */
    public static <T> StreamSource<T> reliableTopicSource(
            @Nonnull String topic,
            int queueCapacity
    ) {
        return SourceBuilder.timestampedStream("streamReliableTopic(" + topic + ")",
                ctx -> new ITopicSourceContext<T>(ctx, topic, queueCapacity))
                .<T>fillBufferFn(ITopicSourceContext::fillBuffer)
                .build();
    }


    private static final class ITopicSourceContext<T> {
        private static final int MAX_FILL_MESSAGES = 512;
        private final List<Message<T>> tempBuffer = new ArrayList<>(MAX_FILL_MESSAGES);
        private BlockingQueue<Message<T>> queue;

        private ITopicSourceContext(Context ctx, String topic, int queueCapacity) {
            queue = new ArrayBlockingQueue<>(queueCapacity);
            ctx.jetInstance().getHazelcastInstance().getRingbuffer(topic);
            ctx.jetInstance().<T>getReliableTopic(topic).addMessageListener(queue::add);
        }

        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) {
            queue.drainTo(tempBuffer, MAX_FILL_MESSAGES);
            tempBuffer.forEach(message -> sourceBuffer.add(message.getMessageObject(), message.getPublishTime()));
            tempBuffer.clear();
        }
    }
}
