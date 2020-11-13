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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService;
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.pipeline.Sink;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for Mqtt sinks.
 */
public final class MqttSinks {

    private MqttSinks() {
    }

    /**
     * Returns a builder object which offers a step-by-step fluent API to build
     * a custom Mqtt {@link Sink sink} for the Pipeline API.
     * <p>
     * The sink creates a
     * <a href="https://www.eclipse.org/paho/clients/java/">Paho</a> client for
     * each processor with memory persistence {@link ConcurrentMemoryPersistence}.
     * Sink generates a random client id if not provided and appends the
     * global processor index to the generated/provided client id to make each
     * created client unique, eg: `the-client-0`, `the-client-1`...
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T> MqttSinkBuilder<T> builder() {
        return new MqttSinkBuilder<>();
    }

    /**
     * Creates a sink which connects to the local broker and publishes to the
     * given topic.The sink converts each item to {@link MqttMessage} using
     * {@link Object#toString()} and {@link String#getBytes()}. The quality of
     * service value of the messages are {@link QualityOfService#AT_LEAST_ONCE}.
     * <p>
     * Useful for quick prototyping. See other methods
     * {@link #mqtt(String, String, FunctionEx)} and {@link #builder()}.
     * <p>
     * For example:
     * <pre>{@code
     * pipeline.writeTo(MqttSinks.publish("topic"));
     * }</pre>
     *
     * @param topic the topic which the sink publishes.
     * @param <T>   the type of the pipeline item.
     */
    @Nonnull
    public static <T> Sink<T> mqtt(String topic) {
        return MqttSinks.<T>builder().topic(topic).build();
    }

    /**
     * Creates a sink which connects to the given broker and publishes to the
     * given topic.The sink converts each item to {@link MqttMessage} using
     * {@link Object#toString()} and {@link String#getBytes()}. The quality of
     * service value of the messages are {@link QualityOfService#AT_LEAST_ONCE}.
     * <p>
     * For example:
     * <pre>{@code
     * pipeline.writeTo(MqttSinks.publish("tcp://localhost:1883", "topic"));
     * }</pre>
     *
     * @param broker the address of the server to connect to, specified as a URI.
     * @param topic  the topic which the sink publishes.
     * @param <T>    the type of the pipeline item.
     */
    @Nonnull
    public static <T> Sink<T> mqtt(@Nonnull String broker, @Nonnull String topic) {
        return MqttSinks.<T>builder().broker(broker).topic(topic).build();
    }

    /**
     * Creates a sink which connects to the given broker and publishes to the
     * given topic. The sink converts each item to {@link MqttMessage} using
     * the given {@code messageFn}.
     * <p>
     * For example, to publish to the `topic` by converting each item to a
     * message with EXACTLY_ONCE quality of service using the item's string
     * representation:
     * <pre>{@code
     * pipeline.writeTo(
     *      MqttSinks.publish(
     *          "tcp://localhost:1883",
     *          "topic",
     *          item -> {
     *              MqttMessage message = new MqttMessage(item.toString().getBytes());
     *              message.setQos(2); // '2' means EXACTLY_ONCE quality of service
     *              return message;
     *          }
     *      )
     * );
     * }</pre>
     *
     * @param broker    the address of the server to connect to, specified as a
     *                  URI.
     * @param topic     the topic which the sink publishes.
     * @param messageFn the function which converts the pipeline items to
     *                  messages.
     * @param <T>       the type of the pipeline item.
     */
    @Nonnull
    public static <T> Sink<T> mqtt(
            @Nonnull String broker,
            @Nonnull String topic,
            @Nonnull FunctionEx<T, MqttMessage> messageFn
    ) {
        return builder().broker(broker).topic(topic).messageFn(messageFn).build();
    }
}
