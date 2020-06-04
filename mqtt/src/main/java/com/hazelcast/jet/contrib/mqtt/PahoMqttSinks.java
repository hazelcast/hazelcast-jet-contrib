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
import com.hazelcast.jet.contrib.mqtt.impl.paho.ConcurrentMemoryPersistence;
import com.hazelcast.jet.contrib.mqtt.impl.paho.SinkCallback;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;

/**
 * Contains factory methods for Mqtt sinks.
 */
public final class PahoMqttSinks {

    private PahoMqttSinks() {
    }

    public static Sink<String> publish(String broker, String topic) {
        return publish(broker, newUnsecureUuidString(), topic, MqttConnectOptions::new, s -> new MqttMessage(s.getBytes()));
    }

    public static <T> Sink<T> publish(
            String broker,
            String clientId,
            String topic,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            FunctionEx<T, MqttMessage> messageFn
    ) {
        return SinkBuilder
                .sinkBuilder("mqttSink", context -> new MqttSinkContext<>(context, broker, clientId, topic, connectOpsFn, messageFn))
                .<T>receiveFn(MqttSinkContext::publish)
                .destroyFn(MqttSinkContext::close)
                .build();
    }

    static class MqttSinkContext<T> {

        private final String topic;
        private final SinkCallback callback;
        private final IMqttAsyncClient client;
        private final FunctionEx<T, MqttMessage> messageFn;

        public MqttSinkContext(
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
            clientId = clientId + "_" + context.globalProcessorIndex();
            IMqttAsyncClient client = new MqttAsyncClient(broker, clientId, new ConcurrentMemoryPersistence());
            client.setCallback(callback);
            client.connect(connectOptions).waitForCompletion();
            return client;
        }
    }
}
