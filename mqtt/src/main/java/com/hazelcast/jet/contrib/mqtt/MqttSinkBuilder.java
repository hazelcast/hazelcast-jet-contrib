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
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.contrib.mqtt.impl.SinkCallback;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * See {@link MqttSinks#builder()} and {@link MqttBaseBuilder}.
 *
 * @param <T> the type of the pipeline item.
 *
 * @since 4.3
 */
public final class MqttSinkBuilder<T> extends MqttBaseBuilder<T> {

    private FunctionEx<T, MqttMessage> messageFn;

    MqttSinkBuilder() {
    }

    /**
     * See {@link MqttBaseBuilder#broker(String)}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> broker(@Nonnull String broker) {
        return (MqttSinkBuilder<T>) super.broker(broker);
    }

    /**
     * See {@link MqttBaseBuilder#clientId(String)}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> clientId(@Nonnull String clientId) {
        return (MqttSinkBuilder<T>) super.clientId(clientId);
    }

    /**
     * See {@link MqttBaseBuilder#autoReconnect()}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> autoReconnect() {
        return (MqttSinkBuilder<T>) super.autoReconnect();
    }

    /**
     * See {@link MqttBaseBuilder#keepSession()}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> keepSession() {
        return (MqttSinkBuilder<T>) super.keepSession();
    }

    /**
     * See {@link MqttBaseBuilder#auth(String, char[])}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> auth(@Nonnull String username, @Nonnull char[] password) {
        return (MqttSinkBuilder<T>) super.auth(username, password);
    }

    /**
     * See {@link MqttBaseBuilder#connectOptionsFn(SupplierEx)}.
     */
    @Nonnull
    @Override
    public MqttSinkBuilder<T> connectOptionsFn(@Nonnull SupplierEx<MqttConnectOptions> connectOptionsFn) {
        return (MqttSinkBuilder<T>) super.connectOptionsFn(connectOptionsFn);
    }

    /**
     * Set the topic which the sink will publish to. This parameter is
     * required.
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
        return (MqttSinkBuilder<T>) super.topic(topic);
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

    static class SinkContext<T> {

        private final String topic;
        private final SinkCallback callback;
        private final IMqttAsyncClient client;
        private final FunctionEx<T, MqttMessage> messageFn;

        SinkContext(
                Processor.Context context,
                String broker,
                String clientId,
                String topic,
                SupplierEx<MqttConnectOptions> connectOpsFn,
                FunctionEx<T, MqttMessage> messageFn
        ) throws MqttException {
            this.topic = topic;
            this.messageFn = messageFn;
            MqttConnectOptions connectOptions = connectOpsFn.get();
            this.callback = new SinkCallback(context.logger(), connectOptions);
            this.client = client(context, broker, clientId, connectOptions);
        }

        public void publish(T item) throws MqttException, InterruptedException {
            callback.acquire();
            client.publish(topic, messageFn.apply(item));
        }

        public void close() throws MqttException {
            client.disconnect().waitForCompletion();
            client.close();
        }

        IMqttAsyncClient client(Processor.Context context, String broker, String clientId,
                                MqttConnectOptions connectOptions) throws MqttException {
            clientId = clientId + "-" + context.globalProcessorIndex();
            IMqttAsyncClient client = new MqttAsyncClient(broker, clientId, new ConcurrentMemoryPersistence());
            client.setCallback(callback);
            client.connect(connectOptions).waitForCompletion();
            return client;
        }
    }
}
