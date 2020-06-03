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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;

/**
 * Contains factory methods for Mqtt sinks.
 */
public final class MqttSinks {

    private MqttSinks() {
    }

    public static Sink<String> publish(String broker, String topic) {
        return publish(broker, topic, newUnsecureUuidString(), MqttConnectOptions::new, s -> new MqttMessage(s.getBytes()));
    }

    public static <T> Sink<T> publish(
            String broker,
            String topic,
            String clientId,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            FunctionEx<T, MqttMessage> messageFn
    ) {
        return SinkBuilder
                .sinkBuilder("mqttSink", context -> new MqttSinkContext<>(broker, topic, clientId, context, connectOpsFn, messageFn))
                .<T>receiveFn(MqttSinkContext::publish)
                .flushFn(MqttSinkContext::flush)
                .destroyFn(MqttSinkContext::close)
                .build();
    }

    static IMqttAsyncClient client(String broker, String clientId) throws MqttException {
        return new MqttAsyncClient(broker, clientId, new ConcurrentMemoryPersistence());
    }

    static class MqttSinkContext<T> implements MqttCallback {


        private final String topic;
        private final int maxInflight;
        private final Semaphore semaphore;
        private final ILogger logger;
        private final IMqttAsyncClient client;
        private final FunctionEx<T, MqttMessage> messageFn;

        public MqttSinkContext(
                String broker,
                String topic,
                String clientId,
                Processor.Context context,
                SupplierEx<MqttConnectOptions> connectOpsFn,
                FunctionEx<T, MqttMessage> messageFn
        ) throws MqttException {
            this.topic = topic;
            this.messageFn = messageFn;
            this.logger = context.logger();

            this.client = client(broker, clientId + "_" + context.globalProcessorIndex());
            MqttConnectOptions connectOptions = connectOpsFn.get();
            maxInflight = connectOptions.getMaxInflight();
            semaphore = new Semaphore(maxInflight);
            client.connect(connectOptions).waitForCompletion();
            client.setCallback(this);
        }


        public void publish(T item) throws MqttException, InterruptedException {
            semaphore.acquire();
            client.publish(topic, messageFn.apply(item));
        }

        public void flush() {
            long i = 1;
            while (semaphore.availablePermits() != maxInflight) {
                LockSupport.parkNanos(i);
                if (i < 500_000) {
                    i = i << 1;
                }
            }
        }

        public void close() throws MqttException {
            client.disconnect().waitForCompletion();
            client.close();
        }

        @Override
        public void connectionLost(Throwable cause) {
            logger.warning(cause);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            semaphore.release();
        }
    }
}
