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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.topic.Message;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;

import java.util.Queue;



/**
 * Contains a method for creating a reliable topic stream sources.
 */
public final class ReliableTopicSource {

    private ReliableTopicSource() {
    }

    /**
     * Creates a {@link StreamSource} which reads messages from
     * from a Hazelcast Reliable Topic with the specified name.
     * @param topic       topic name to read from
     * @param projectionFn built in mapping function of the source which can be
     *                     used to map {@link Message} instances received
     *                     from a reliable topic to an arbitrary type of output;
     *                     it's included for convenience.
     * @param <M> type of item inside a message.
     * @param <T> type of emitted item after projection.
     * @return {@link StreamSource}
     */
    public static <M, T> StreamSource<T> topicSource(
            @Nonnull String topic,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        return SourceBuilder.timestampedStream("reliable-topic-source",
                ctx -> new ITopicSourceContext<>(ctx, topic, projectionFn))
                .<T>fillBufferFn(ITopicSourceContext::fillBuffer)
                .build();
    }

    private static final class ITopicSourceContext<M, T> {
        private static final int QUEUE_CAP = 1024;
        private static final int MAX_FILL_MESSAGES = 128;

        private final Queue<Message<M>> queue = new ArrayDeque<>(QUEUE_CAP);
        private final FunctionEx<Message<M>, T> projectionFn;

        private ITopicSourceContext(Context ctx, String topic, @Nonnull FunctionEx<Message<M>, T> projectionFn) {
            ctx.jetInstance().<M>getReliableTopic(topic).addMessageListener(queue::add);
            this.projectionFn = projectionFn;
        }

        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) {
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_MESSAGES) {
                Message<M> message = queue.poll();
                long timestamp = message.getPublishTime();
                T item = projectionFn.apply(message);
                if (item != null) {
                    sourceBuffer.add(item, timestamp);
                }
            }
        }
    }
}
