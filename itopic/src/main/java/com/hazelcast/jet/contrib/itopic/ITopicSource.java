package com.hazelcast.jet.contrib.itopic;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.topic.Message;

import javax.annotation.Nonnull;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class ITopicSource {
    private ITopicSource() {
    }

    public static <M, T> StreamSource<T> topicSource(
            @Nonnull String topicName,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        return SourceBuilder.timestampedStream("reliable-topic-source",
                ctx -> new ITopicSourceContext<>(ctx, topicName, projectionFn))
                .<T>fillBufferFn(ITopicSourceContext::fillBuffer)
                .build();
    }

    private static final class ITopicSourceContext<M, T> {
        private static final int QUEUE_CAP = 1024;
        private static final int MAX_FILL_MESSAGES = 128;

        private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(QUEUE_CAP);
        private final FunctionEx<Message<M>, T> projectionFn;

        public ITopicSourceContext(Context ctx, String topicName, @Nonnull FunctionEx<Message<M>, T> projectionFn) {
            ctx.jetInstance().getReliableTopic(topicName).addMessageListener(queue::add);
            this.projectionFn = projectionFn;
        }

        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) {
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_MESSAGES) {
                Message<M> message = (Message<M>) queue.poll();
                long timestamp = message.getPublishTime();
                T item = projectionFn.apply(message);
                if (item != null) {
                    sourceBuffer.add(item, timestamp);
                }
            }
        }
    }
}
