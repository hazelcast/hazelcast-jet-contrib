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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.mqtt.impl.SinkContext;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * See {@link MqttSinks#builder()} and {@link AbstractMqttBuilder}.
 *
 * @param <T> the type of the pipeline item.
 * @since 4.3
 */
public final class MqttSinkBuilder<T> extends AbstractMqttBuilder<T, MqttSinkBuilder<T>> {

    private FunctionEx<T, MqttMessage> messageFn;

    MqttSinkBuilder() {
    }

    /**
     * Set the topic which the sink will publish to. The topic should
     * not contain any wildcards. This parameter is required.
     * <p>
     * For example, to publish to the house topic:
     * <pre>{@code
     * builder.topic("house")
     * }</pre>
     * <p>
     *
     * @param topic the topic which the sink publishes.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> topic(@Nonnull String topic) {
        return super.topic(topic);
    }

    /**
     * Set the function to map a pipeline item to {@link MqttMessage}.
     * <p>
     * For example, to convert each item to a message with EXACTLY_ONCE quality
     * of service using its string representation:
     * <pre>{@code
     * builder.messageFn(
     *      item -> {
     *          MqttMessage message = new MqttMessage(item.toString().getBytes());
     *          message.setQos(2); // '2' means EXACTLY_ONCE quality of service
     *          return message;
     *      }
     * )
     * }</pre>
     * <p>
     * If not set, each item is converted to a message with AT_LEAST_ONCE
     * quality of service using its string representation.
     * <p>
     * See {@link MqttMessage#setQos(int)}.
     *
     * @param messageFn the function which converts the pipeline items to
     *                  messages.
     * @param <T_NEW>   the type of the pipeline item.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> MqttSinkBuilder<T_NEW> messageFn(@Nonnull FunctionEx<T_NEW, MqttMessage> messageFn) {
        MqttSinkBuilder<T_NEW> newThis = (MqttSinkBuilder<T_NEW>) this;
        newThis.messageFn = messageFn;
        return newThis;
    }

    /**
     * Build a Mqtt {@link Sink} with the supplied parameters.
     */
    @Nonnull
    public Sink<T> build() {
        String localBroker = broker;
        String localClientId = clientId;
        String localTopic = requireNonNull(topic, "topic must be set");

        SupplierEx<MqttConnectOptions> connectOpsFn = connectOpsFn();
        FunctionEx<T, MqttMessage> mesFn = mesFn();
        return SinkBuilder.sinkBuilder("mqttSink", context -> new SinkContext<>(
                context, localBroker, localClientId, localTopic, connectOpsFn, mesFn))
                .<T>receiveFn(SinkContext::publish)
                .destroyFn(SinkContext::close)
                .build();
    }

    private FunctionEx<T, MqttMessage> mesFn() {
        if (messageFn != null) {
            return messageFn;
        }
        return item -> new MqttMessage(item.toString().getBytes());
    }

}
