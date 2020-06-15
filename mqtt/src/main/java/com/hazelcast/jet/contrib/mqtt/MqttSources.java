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

package com.hazelcast.jet.contrib.mqtt;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService;
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for Mqtt sources.
 * Alternatively you can use {@link MqttSourceBuilder}
 *
 * @since 4.2
 */
public final class MqttSources {

    private MqttSources() {
    }

    /**
     * Returns a builder object which offers a step-by-step fluent API to build
     * a custom Mqtt {@link StreamSource source} for the Pipeline API.
     * <p>
     * The source creates a
     * <a href="https://www.eclipse.org/paho/clients/java/">Paho</a> client for
     * each specified topic with memory persistence {@link ConcurrentMemoryPersistence}.
     * Source generates a random client id if not provided and appends the
     * global processor index to the generated/provided client id to make each
     * created client unique, eg: `the-client-0`, `the-client-1`...
     * <p>
     * The source is not distributed if a single topic provided, otherwise
     * topics are distributed among members. If you use wildcard in your topic
     * it is still considered by the source as a single topic.
     * <p>
     * The source is not re-playable from a certain offset thus it cannot
     * participate in snapshotting and not fault tolerant. But if you set
     * {@link MqttConnectOptions#setCleanSession(boolean)} to {@code false}
     * The broker will keep the published messages with quality service above
     * `0` and deliver them to the source after restart.
     * <p>
     * The source emits items of type {@code byte[]}, the payload of the
     * message, if {@link MqttSourceBuilder#mapToItemFn(BiFunctionEx)} is not
     * set.
     */
    @Nonnull
    public static MqttSourceBuilder<byte[]> builder() {
        return new MqttSourceBuilder<>();
    }

    /**
     * Creates a streaming source which connects to the local broker and
     * subscribes to the given topic with {@link QualityOfService#AT_LEAST_ONCE}.
     * <p>
     * Useful for quick prototyping. See other methods
     * {@link #subscribe(String, Subscription, BiFunctionEx)} and
     * {@link #builder()}
     * <p>
     * For example:
     * <pre>{@code
     * pipeline.readFrom(MqttSources.subscribe("topic"));
     * }</pre>
     *
     * @param topic the topic which the source subscribes, may include
     *              wildcards.
     */
    @Nonnull
    public static StreamSource<byte[]> subscribe(@Nonnull String topic) {
        return builder().topic(topic).build();
    }

    /**
     * Creates a streaming source which connects to the given broker and
     * subscribes to the given topic with {@link QualityOfService#AT_LEAST_ONCE}.
     * <p>
     * For example:
     * <pre>{@code
     * pipeline.readFrom(MqttSources.subscribe("tcp://localhost:1883", "topic"));
     * }</pre>
     *
     * @param broker the address of the server to connect to, specified as a URI.
     * @param topic  the topic which the source subscribes, may include wildcards.
     */
    @Nonnull
    public static StreamSource<byte[]> subscribe(@Nonnull String broker, @Nonnull String topic) {
        return builder().broker(broker).topic(topic).build();
    }

    /**
     * Creates a streaming source which connects to the given broker and
     * subscribes using given {@link Subscription}. The source converts
     * messages to the desired output object using given {@code mapToItemFn}.
     * <p>
     * For example, to subscribe to the `topic` with `EXACTLY_ONCE` quality of
     * service and convert each message to a string:
     * <pre>{@code
     * pipeline.readFrom(
     *      MqttSources.subscribe(
     *          "tcp://localhost:1883",
     *          Subscription.of("topic", EXACTLY_ONCE),
     *          (t, m) -> new String(m.getPayload())
     *      )
     * )
     * }</pre>
     *
     * @param broker       the address of the server to connect to, specified
     *                     as a URI.
     * @param subscription the topic which the source subscribes and its
     *                     quality of service value, the topic may include
     *                     wildcards.
     * @param mapToItemFn  the function which converts the messages to pipeline
     *                     items.
     * @param <T>          type of the pipeline items emitted to downstream.
     */
    public static <T> StreamSource<T> subscribe(
            @Nonnull String broker,
            @Nonnull Subscription subscription,
            @Nonnull BiFunctionEx<String, MqttMessage, T> mapToItemFn
    ) {
        return builder()
                .broker(broker)
                .topic(subscription.getTopic())
                .qualityOfService(subscription.getQualityOfService())
                .mapToItemFn(mapToItemFn)
                .build();
    }
}
