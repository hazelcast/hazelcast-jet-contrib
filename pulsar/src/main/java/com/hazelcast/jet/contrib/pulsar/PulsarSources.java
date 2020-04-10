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

package com.hazelcast.jet.contrib.pulsar;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Contains methods for creating Pulsar stream sources.
 */
public final class PulsarSources {
    private PulsarSources() {
    }

    /**
     * Creates a distributed, timestamped {@link StreamSource} which reads
     * messages from Pulsar topics for data ingestion to Jet pipelines.
     * This source does not have fault-tolerance support. It uses the
     * Consumer API of the Pulsar client. It can be used to subscribe
     * partitioned topics. It uses higher level abstraction of Pulsar that
     * is called "shared subscription" that allows multiple consumers to consume
     * from the topic at the same time. The messages are sent round-robin to each
     * connected consumer. Broker determines which consumer will receive a
     * message from which topic partition. It does not require one-to-one
     * mapping between partitions and consumers. Multiple consumers can get
     * messages from same partition. With this source, the message ordering is
     * not preserved.
     * <p>
     * Example usage:
     * <pre>{@code
     *
     *   Map<String, Object> consumerConfig = new HashMap<>();
     *   consumerConfig.put("consumerName", "hazelcast-jet-consumer");
     *   consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
     *   StreamSource<String> pulsarSource = PulsarSources.pulsarDistributed(
     *          Collections.singletonList(topicName),
     *          2,       // Preferred Local Parallelism
     *          consumerConfig,
     *          () -> PulsarClient.builder()
     *                            .serviceUrl("pulsar://exampleserviceurl")
     *                            .build(), // Client Supplier
     *          () -> Schema.BYTES, // Schema Supplier Function
     *          x -> new String(x.getData(), StandardCharsets.UTF_8)
     *                                       // Projection function that converts
     *                                       // receiving bytes to String
     *                                       // before emitting.
     *          );
     *  Pipeline pipeline = Pipeline.create();
     *  StreamStage<Status> srcStage = p.readFrom(pulsarSource);
     *
     *  }</pre>
     *
     * @param topics the topics to consume, at least one is required
     * @param preferredLocalParallelism the preferred number of local
     *                                                    parallelism
     * @param consumerConfig Pulsar consumer configurations that must
     *                       contain consumer name, and subscription name.
     * @param schemaSupplier supplies the schema for consuming messages
     * @param batchReceivePolicySupplier supplies the batch receiver policy for consumer
     * @param projectionFn converts a Pulsar message to an emitted item.
     * @param <M> the type of the message read by {@code PulsarConsumer}
     * @param <T> the type of data emitted from {@code StreamSource}
     * @return a stream source to use in {@link Pipeline#readFrom}
     */
    public static <M, T> StreamSource<T> pulsarConsumer(
            @Nonnull List<String> topics,
            int preferredLocalParallelism,
            @Nonnull Map<String, Object> consumerConfig,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        return SourceBuilder.timestampedStream("pulsar-consumer-source", ctx -> new ConsumerContext<>(
                ctx.logger(), connectionSupplier.get(), topics, consumerConfig,
                schemaSupplier, batchReceivePolicySupplier, projectionFn))
                .<T>fillBufferFn(ConsumerContext::fillBuffer)
                .destroyFn(ConsumerContext::destroy)
                .distributed(preferredLocalParallelism)
                .build();
    }


    /**
     * Creates a fault-tolerant timestamped {@link StreamSource}
     * which reads messages from Pulsar topics for data ingestion to Jet
     * pipelines. It uses the Reader API of the Pulsar client. It cannot
     * be used in the partitioned topics.
     * <p>
     * Example usage:
     * <pre>{@code
     *      Properties credentials = loadTwitterCredentials();
     *      TimestampedS<Status> twitterSearchSource =
     *      TwitterSources.search(credentials,"Jet flies");
     *      Pipeline p = Pipeline.create();
     *      BatchStage<Status> srcStage = p.readFrom(pulsarSource);
     *  }</pre>
     *
     * @param topic          the single topic to consume
     * @param readerConfig   Pulsar reader configurations that must contain
     *                       reader name
     * @param schemaSupplier supplies the schema for consuming messages
     * @param projectionFn   converts a Pulsar message to an emitted item.
     * @param <M>            the type of the message read by {@code PulsarConsumer}
     * @param <T>            the type of data emitted from {@code StreamSource}
     * @return a stream source to use in {@link Pipeline#readFrom}
     */

    public static <M, T> StreamSource<T> pulsarReader(
            @Nonnull String topic,
            @Nonnull Map<String, Object> readerConfig,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        return SourceBuilder.timestampedStream("pulsar-reader-source", ctx -> new ReaderContext<>(
                ctx.logger(), connectionSupplier.get(), topic, readerConfig, schemaSupplier, projectionFn))
                .<T>fillBufferFn(ReaderContext::fillBuffer)
                .createSnapshotFn(ReaderContext::createSnapshot)
                .restoreSnapshotFn(ReaderContext::restoreSnapshot)
                .destroyFn(ReaderContext::destroy)
                .build();
    }

    /**
     * A context for the stream source of Apache Pulsar
     *
     * @param <M> the type of the value of message read by {@code PulsarConsumer}
     * @param <T> the type of the emitted item after projection.
     */
    private static final class ConsumerContext<M, T> {
        private static final int MAX_ACK_RETRIES = 3;

        private final ILogger logger;
        private final PulsarClient client;
        private final Consumer<M> consumer;
        private final FunctionEx<Message<M>, T> projectionFn;

        private ConsumerContext(
                @Nonnull ILogger logger,
                @Nonnull PulsarClient client,
                @Nonnull List<String> topics,
                @Nonnull Map<String, Object> consumerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) throws PulsarClientException {
            checkSerializable(schemaSupplier, "schemaSupplier");
            checkSerializable(batchReceivePolicySupplier, "batchReceivePolicySupplier");
            checkSerializable(projectionFn, "projectionFn");
            this.logger = logger;
            this.projectionFn = projectionFn;
            this.client = client;
            this.consumer = client.newConsumer(schemaSupplier.get())
                                  .topics(topics)
                                  .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                  .loadConf(consumerConfig)
                                  .batchReceivePolicy(batchReceivePolicySupplier.get())
                                  .subscriptionType(SubscriptionType.Shared)
                                  .subscribe();
        }


        /**
         * Receive the messages as a batch. The {@link BatchReceivePolicy} is
         * configured while creating the Pulsar {@link Consumer}.
         * In this method, emitted items are created by applying the projection function
         * to the messages received from Pulsar client. If there is an event time
         * associated with the message, it sets the event time as the timestamp of the
         * emitted item. Otherwise, it sets the publish time(which always exists)
         * of the message as the timestamp.
         */
        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) throws PulsarClientException {
            Messages<M> messages = consumer.batchReceive();
            for (Message<M> message : messages) {
                if (message.getEventTime() != 0) {
                    sourceBuffer.add(projectionFn.apply(message), message.getEventTime());
                } else {
                    sourceBuffer.add(projectionFn.apply(message), message.getPublishTime());
                }
            }
            acknowledgeWithRetryAsync(messages);
        }

        public void acknowledgeWithRetryAsync(Messages<M> messages) {
            consumer.acknowledgeAsync(messages)
                    .thenApply(CompletableFuture::completedFuture)
                    .exceptionally(t -> retry(messages, t, 0));
        }

        private CompletableFuture<Void> retry(Messages<M> messages, Throwable throwable, int retry) {
            if (retry >= MAX_ACK_RETRIES) {
                logger.warning(buildLogMessage(messages));
                ExceptionUtil.sneakyThrow(throwable);
            }
            return consumer.acknowledgeAsync(messages)
                    .thenApply(CompletableFuture::completedFuture)
                    .exceptionally(t -> {
                        throwable.addSuppressed(t);
                        return retry(messages, t, retry + 1);
                    })
                    .thenCompose(FunctionEx.identity());
        }

        private String buildLogMessage(Messages<M> messages) {
            StringBuilder builder = new StringBuilder();
            builder.append("Received batch with message ids: ");
            String prefix = "";
            for (Message<M> message : messages) {
                builder.append(prefix);
                prefix = ", ";
                builder.append(message.getMessageId());
            }
            builder.append(" cannot be acknowledged.");
            return builder.toString();
        }

        private void destroy() {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                logger.warning("Error while closing the 'PulsarConsumer'.", e);
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                logger.warning("Error while shutting down the 'PulsarClient'.", e);
            }
        }
    }


    /**
     * A context for the Pulsar Reader for the fault-tolerant stream source of Apache Pulsar
     *
     * @param <M> the type of the value of message read by {@code PulsarConsumer}
     * @param <T> the type of the emitted item after projection.
     */
    private static final class ReaderContext<M, T> {
        private static final int QUEUE_CAP = 1024;
        private static final int MAX_FILL_MESSAGES = 128;

        private final ILogger logger;
        private final PulsarClient client;
        private final BlockingQueue<Message<M>> queue = new ArrayBlockingQueue<>(QUEUE_CAP);
        private final ArrayList<Message<M>> buffer = new ArrayList<>(MAX_FILL_MESSAGES);

        private final FunctionEx<Message<M>, T> projectionFn;
        private final Map<String, Object> readerConfig;
        private final Schema<M> schema;
        private final String topic;
        private Reader<M> reader;
        private MessageId offset = MessageId.earliest;

        private ReaderContext(
                @Nonnull ILogger logger,
                @Nonnull PulsarClient client,
                @Nonnull String topic,
                @Nonnull Map<String, Object> readerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) {
            checkSerializable(schemaSupplier, "schemaSupplier");
            checkSerializable(projectionFn, "projectionFn");
            this.logger = logger;
            this.client = client;
            this.topic = topic;
            this.readerConfig = readerConfig;
            this.schema = schemaSupplier.get();
            this.projectionFn = projectionFn;
        }

        /**
         * Receive the messages as a batch.
         * In this method, emitted items are created by applying the projection function
         * to the messages received from Pulsar client. If there is an event time
         * associated with the message, it sets the event time as the timestamp of the
         * emitted item. Otherwise, it sets the publish time(which always exists)
         * of the message as the timestamp.
         */
        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) throws PulsarClientException {
            if (reader == null) {
                createReader();
            }
            queue.drainTo(buffer, MAX_FILL_MESSAGES);
            for (Message<M> message : buffer) {
                long timestamp;
                if (message.getEventTime() != 0) {
                    timestamp = message.getEventTime();
                } else {
                    timestamp = message.getPublishTime();
                }
                T item = projectionFn.apply(message);
                offset = message.getMessageId();
                if (item != null) {
                    sourceBuffer.add(item, timestamp);
                }
            }
            buffer.clear();
        }

        private void createReader() throws PulsarClientException {
            reader = client.newReader(schema)
                    .topic(topic)
                    .startMessageId(offset)
                    .loadConf(readerConfig)
                    .readerListener((r, msg) -> {
                        try {
                            queue.put(msg);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    })
                    .receiverQueueSize(QUEUE_CAP)
                    .create();
        }

        byte[] createSnapshot() {
            return offset.toByteArray();
        }

        void restoreSnapshot(List<byte[]> snapshots) throws IOException {
            offset = MessageId.fromByteArray(snapshots.get(0));
        }

        private void destroy() {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warning("Error while closing the 'Pulsar Reader'.", e);
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                logger.warning("Error while shutting down the 'PulsarClient'.", e);
            }
        }

    }


}
