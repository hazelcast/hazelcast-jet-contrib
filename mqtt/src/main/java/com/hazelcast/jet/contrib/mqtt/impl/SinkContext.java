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

package com.hazelcast.jet.contrib.mqtt.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Arrays;
import java.util.concurrent.Semaphore;

public class SinkContext<T> {

    private final ILogger logger;
    private final String topic;
    private final IMqttAsyncClient client;
    private final FunctionEx<T, MqttMessage> messageFn;
    private final Semaphore semaphore;
    private final IMqttActionListener loggingActionListener;
    private final IMqttActionListener releasingPermitActionListener;

    public SinkContext(
            Processor.Context context,
            String broker,
            String clientId,
            String topic,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            FunctionEx<T, MqttMessage> messageFn
    ) throws MqttException {
        this.logger = context.logger();
        this.topic = topic;
        this.messageFn = messageFn;
        MqttConnectOptions connectOptions = connectOpsFn.get();
        this.semaphore = new Semaphore(connectOptions.getMaxInflight());
        this.client = client(context, broker, clientId, connectOptions);
        this.loggingActionListener = new LoggingActionListener();
        this.releasingPermitActionListener = new ReleasingPermitActionListener();
    }

    public void publish(T item) throws MqttException, InterruptedException {
        MqttMessage message = messageFn.apply(item);
        if (message.getQos() == 0) {
            client.publish(topic, message, null, loggingActionListener);
        } else {
            semaphore.acquire();
            client.publish(topic, message, null, releasingPermitActionListener);
        }
    }

    public void close() throws MqttException {
        client.disconnect().waitForCompletion();
        client.close();
    }

    IMqttAsyncClient client(Processor.Context context, String broker, String clientId,
                            MqttConnectOptions connectOptions) throws MqttException {
        clientId = clientId + "-" + context.globalProcessorIndex();
        IMqttAsyncClient client = new MqttAsyncClient(broker, clientId, new ConcurrentMemoryPersistence());
        client.connect(connectOptions).waitForCompletion();
        return client;
    }

    class LoggingActionListener implements IMqttActionListener {

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            String[] topics = asyncActionToken.getTopics();
            logger.severe("Publishing to " + Arrays.toString(topics) + " failed", exception);
        }
    }

    class ReleasingPermitActionListener extends LoggingActionListener {
        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            semaphore.release();
        }
    }
}
