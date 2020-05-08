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
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;


/**
 * Contains builders for creating Pulsar stream sources.
 */
public final class PulsarSources {
    private PulsarSources() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Pulsar consumer {@link StreamSource} for the Pipeline API.
     * <p>
     * Pulsar consumer source is a distributed, timestamped {@link StreamSource}
     * which reads messages from Pulsar topics for data ingestion to Jet
     * pipelines. This source does not have fault-tolerance support. It uses
     * the Consumer API of the Pulsar client. It can be used to subscribe
     * partitioned topics. It uses higher level abstraction of Pulsar that
     * is called "shared subscription" that allows multiple consumers to consume
     * from the topics at the same time. The messages are sent round-robin to
     * each connected consumer. Broker determines which consumer will receive a
     * message from which topic partition. It does not require one-to-one
     * mapping between partitions and consumers. Multiple consumers can get
     * messages from same partition. With this source, the message ordering is
     * not preserved.
     * <p>
     * Example usage:
     * <pre>{@code
     *
     * StreamSource<String> pulsarSource = PulsarSources.pulsarConsumerBuilder(
     *          Arrays.asList(topicName, topicName2 ...),
     *          () -> PulsarClient.builder()
     *                            .serviceUrl("pulsar://exampleserviceurl")
     *                            .build(), // Client Supplier
     *          () -> Schema.BYTES, // Schema Supplier Function
     *          x -> new String(x.getData(), StandardCharsets.UTF_8)
     *                                       // Projection function that converts
     *                                       // receiving bytes to String
     *                                       // before emitting.
     *          ).build();
     *
     *  Pipeline pipeline = Pipeline.create();
     *  StreamStage<Status> srcStage = p.readFrom(pulsarSource);
     *
     *  }</pre>
     *
     * @param topics             the topics to consume, at least one is required
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     supplies the schema for consuming messages
     * @param projectionFn       converts a Pulsar message to an emitted item.
     * @param <M>                the type of the message read by {@code PulsarConsumer}
     * @param <T>                the type of data emitted from {@code StreamSource}
     * @return {@link PulsarConsumerBuilder} that used to create a {@link StreamSource}
     */

    public static <M, T> PulsarConsumerBuilder<M, T> pulsarConsumerBuilder(
            @Nonnull List<String> topics,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn

    ) {
        return new PulsarConsumerBuilder<>(topics, connectionSupplier, schemaSupplier, projectionFn);
    }

    /**
     * See the {@link #pulsarConsumerBuilder(List, SupplierEx, SupplierEx, FunctionEx)}
     * It gets a single String for the topic name in case it should read only
     * from a single topic.
     *
     * @param topic the single topic to consume
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     supplies the schema for consuming messages
     * @param projectionFn       converts a Pulsar message to an emitted item.
     * @param <M>                the type of the message read by {@code PulsarConsumer}
     * @param <T>                the type of data emitted from {@code StreamSource}
     * @return {@link PulsarConsumerBuilder} that used to create a {@link StreamSource}
     */
    public static <M, T> PulsarConsumerBuilder<M, T> pulsarConsumerBuilder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn

    ) {
        return pulsarConsumerBuilder(Collections.singletonList(topic), connectionSupplier, schemaSupplier, projectionFn);
    }


    /**
     * Convenience for {@link #pulsarConsumerBuilder(String, SupplierEx, SupplierEx, FunctionEx)}.
     * It creates a basic Pulsar consumer that connects the topic by using Pulsar client.
     * <p>
     *
     * @param topic              the single topic to consume
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     supplies the schema for consuming messages
     * @param projectionFn       converts a Pulsar message to an emitted item.
     * @param <M>                the type of the message read by {@code pulsarReader}
     * @param <T>                the type of data emitted from {@code StreamSource}
     * @return {@link StreamSource}
     */
    public static <M, T> StreamSource<T> pulsarConsumer(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        return PulsarSources.pulsarConsumerBuilder(topic, connectionSupplier, schemaSupplier, projectionFn)
                            .build();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Pulsar reader {@link StreamSource} for the Pipeline API.
     * <p>
     * Pulsar reader is a fault-tolerant timestamped {@link StreamSource}
     * which reads messages from Pulsar topics for data ingestion to Jet
     * pipelines. It uses the Reader API of the Pulsar client. It cannot
     * be used in the partitioned topics.
     * <p>
     * Example usage:
     * <pre>{@code
     *
     * StreamSource<String> pulsarSource = PulsarSources.pulsarReaderBuilder(
     *          topicName,
     *          () -> PulsarClient.builder()
     *                            .serviceUrl("pulsar://exampleserviceurl")
     *                            .build(), // Client Supplier
     *          () -> Schema.BYTES, // Schema Supplier Function
     *          x -> new String(x.getData(), StandardCharsets.UTF_8)
     *                                       // Projection function that converts
     *                                       // receiving bytes to String
     *                                       // before emitting.
     *          ).build();
     *
     *  Pipeline pipeline = Pipeline.create();
     *  StreamStage<Status> srcStage = p.readFrom(pulsarSource);
     *
     *  }</pre>
     *
     * @param topic              the single topic to consume
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     supplies the schema for consuming messages
     * @param projectionFn       converts a Pulsar message to an emitted item.
     * @param <M>                the type of the message read by {@code pulsarReader}
     * @param <T>                the type of data emitted from {@code StreamSource}
     * @return {@link PulsarReaderBuilder} that used to create a {@link StreamSource}
     */
    public static <M, T> PulsarReaderBuilder<M, T> pulsarReaderBuilder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        return new PulsarReaderBuilder<>(topic, connectionSupplier, schemaSupplier, projectionFn);
    }


    /**
     * Convenience for {@link #pulsarReaderBuilder(String, SupplierEx, SupplierEx, FunctionEx)}.
     * It creates a basic Pulsar reader that connects the topic by using Pulsar client.
     * <p>
     *
     * @param topic              the single topic to consume
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     supplies the schema for consuming messages
     * @param projectionFn       converts a Pulsar message to an emitted item.
     * @param <M>                the type of the message read by {@code pulsarReader}
     * @param <T>                the type of data emitted from {@code StreamSource}
     * @return {@link StreamSource}
     */
    public static <M, T> StreamSource<T> pulsarReader(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn
    ) {
        return PulsarSources.pulsarReaderBuilder(topic, connectionSupplier, schemaSupplier, projectionFn)
                            .build();
    }


}
