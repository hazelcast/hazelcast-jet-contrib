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

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;


/**
 * Contains methods for creating ITopic sink.
 */
public final class ReliableTopicSink {

    private ReliableTopicSink() {
    }

    /**
     * Creates a {@link Sink} which publish messages to
     * a Hazelcast reliable topic with the specified name.
     * @param topic topic name to publish to
     * @param <T> type of published item
     * @return {@link Sink}
     */
    public static <T> Sink<T> topicSink(
            @Nonnull String topic
    ) {
        return SinkBuilder.<ITopic<T>>sinkBuilder("reliable-topic-sink",
                ctx -> ctx.jetInstance().getReliableTopic(topic))
                .<T>receiveFn(ITopic::publish)
                .build();
    }
}
