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

package com.hazelcast.jet.contrib.pulsar;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;


import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Contains methods for creating Pulsar stream sources.
 */
public final class PulsarSources {
    private PulsarSources() {
    }

    /**
     * Creates a timestamped {@link StreamSource} which reads messages from Pulsar
     * topics for data ingestion to Jet pipelines.
     *
     * @param topics         the topics to consume, at least one is required
     * @param consumerConfig Pulsar consumer configurations that must contain
     *                       consumer name, subscription name.
     * @param schemaSupplier supplies the schema for consuming messages
     * @param projectionFn   converts a Pulsar message to an emitted item.
     * @param <M>            the type of the message read by {@code PulsarConsumer}
     * @param <T>            the type of data emitted from {@code StreamSource}
     * @return a stream source to use in {@link Pipeline#readFrom}
     */
    public static <M, T> StreamSource<T> subscribe(
            @Nonnull List<String> topics,
            @Nonnull Map<String, Object> consumerConfig,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        return SourceBuilder.timestampedStream("pulsar-ts-stream-source", ctx -> new PulsarSourceContext(
                connectionSupplier.get(), topics, consumerConfig, schemaSupplier, projectionFn))
                .<T>fillBufferFn(PulsarSourceContext::fillBuffer)
                .destroyFn(PulsarSourceContext::destroy)
                .build();
    }

    /**
     * A context for the stream source of Apache Pulsar
     *
     * @param <M> the type of the value of message read by {@code PulsarConsumer}
     * @param <T> the type of the emitted item after projection.
     */
    private static final class PulsarSourceContext<M, T> {
        private final PulsarClient client;
        private final Consumer<M> consumer;
        private final FunctionEx<Message<M>, T> projectionFn;

        private PulsarSourceContext(
                @Nonnull PulsarClient client,
                @Nonnull List<String> topics,
                @Nonnull Map<String, Object> consumerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) throws PulsarClientException {
            checkSerializable(schemaSupplier, "schemaSupplier");
            checkSerializable(projectionFn, "projectionFn");
            this.projectionFn = projectionFn;
            this.client = client;
            this.consumer = client
                    .newConsumer(schemaSupplier.get())
                    .topics(topics)
                    .loadConf(consumerConfig)
                    .subscribe();
        }

        /**
         * If there is an event time associated with the message, set the
         * event time as the timestamp. Otherwise, it will set the publish time
         * of this message as the timestamp.
         */
        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) {
            CompletableFuture<Message<M>> messageFuture = consumer.receiveAsync();
            messageFuture.thenAccept(message -> {
                        if (message.getEventTime() != 0) {
                            sourceBuffer.add(projectionFn.apply(message), message.getEventTime());
                        } else {
                            sourceBuffer.add(projectionFn.apply(message), message.getPublishTime());
                        }
                    }
            );
        }

        private void destroy() {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }
    }
}
