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

import com.hazelcast.jet.pipeline.Sink;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Contains factory methods for Pulsar sinks.
 */
public final class PulsarSinks {

    private PulsarSinks() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Pulsar {@link Sink} for the Pipeline API.
     *
     * @param topic              Pulsar topic name to publish to
     * @param producerConfig     The configurations are used to create the {@code PulsarProducer}
     * @param connectionSupplier Pulsar client supplier
     * @param <E>                the type of stream items that sink accepts
     * @param <M>                the type of the message published by {@code PulsarProducer}
     */
    public static <E, M> PulsarSinkBuilder<E, M> builder(
            @Nonnull String topic,
            @Nonnull Map<String, Object> producerConfig,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier
    ) {
        return new PulsarSinkBuilder<>(topic, producerConfig, connectionSupplier);
    }

    /**
     * Convenience for {@link #builder(String, Map, SupplierEx)}.
     * It creates a basic Pulsar sink that connect the topic.
     */
    public static <E, M> Sink<E> pulsarSink(
            @Nonnull String topic,
            @Nonnull String serviceUrl,
            @Nonnull Map<String, Object> producerConfig,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<? super E, M> extractValueFn
    ) {

        return PulsarSinks.<E, M>builder(topic, producerConfig, () -> PulsarClient.builder()
                                                                                  .serviceUrl(serviceUrl)
                                                                                  .build())
                .schemaSupplier(schemaSupplier)
                .extractValueFn(extractValueFn)
                .build();
    }

}
