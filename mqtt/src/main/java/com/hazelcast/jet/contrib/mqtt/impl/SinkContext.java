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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SinkContext<T> {

    private static final long WAIT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private final ILogger logger;
    private final String topic;
    private final IMqttAsyncClient client;
    private final FunctionEx<T, MqttMessage> messageFn;
    private final BiFunctionEx<MqttException, Integer, Long> retryFn;

    public SinkContext(
            Processor.Context context,
            String broker,
            String clientId,
            String topic,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            BiFunctionEx<MqttException, Integer, Long> retryFn,
            FunctionEx<T, MqttMessage> messageFn
    ) throws MqttException {
        this.logger = context.logger();
        this.topic = topic;
        this.retryFn = retryFn;
        this.messageFn = messageFn;
        this.client = client(context, broker, clientId, connectOpsFn.get());
    }

    public void publish(T item) throws MqttException, InterruptedException {
        MqttMessage message = messageFn.apply(item);
        int tryCount = 0;
        while (true) {
            try {
                client.publish(topic, message).waitForCompletion(WAIT_TIMEOUT_MILLIS);
                return;
            } catch (MqttException exception) {
                long retryWaitTimeMillis = retryFn.apply(exception, tryCount);
                if (retryWaitTimeMillis <= 0) {
                    throw exception;
                }
                tryCount++;
                logger.warning(
                        String.format("Exception while publishing %s to %s, try count: %d", item, topic, tryCount),
                        exception
                );
                MILLISECONDS.sleep(retryWaitTimeMillis);
            }
        }
    }

    public void close() throws MqttException {
        client.disconnect().waitForCompletion();
        client.close();
    }

    private IMqttAsyncClient client(Processor.Context context, String broker, String clientId,
                                    MqttConnectOptions connectOptions) throws MqttException {
        clientId = clientId + "-" + context.globalProcessorIndex();
        IMqttAsyncClient client = new MqttAsyncClient(broker, clientId, new ConcurrentMemoryPersistence());
        client.connect(connectOptions).waitForCompletion();
        return client;
    }

}
