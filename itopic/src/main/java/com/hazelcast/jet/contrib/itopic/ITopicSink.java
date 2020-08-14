package com.hazelcast.jet.contrib.itopic;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;


/**
 * Contains methods for creating ITopic sink.
 */
public class ITopicSink {

    private ITopicSink() {
    }


    public <T> Sink<T> ITopicSink(
            @Nonnull String topicName
    ) {
        return SinkBuilder.<ITopic<T>>sinkBuilder("reliable-topic-sink",
                ctx -> ctx.jetInstance().getReliableTopic(topicName))
                .<T>receiveFn(ITopic::publish)
                .build();
    }




}
