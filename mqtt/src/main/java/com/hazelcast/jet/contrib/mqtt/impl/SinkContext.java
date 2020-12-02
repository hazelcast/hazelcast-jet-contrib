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
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategy;
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
    private final RetryStrategy retryStrategy;
    private final FunctionEx<T, MqttMessage> messageFn;

    public SinkContext(
            Processor.Context context,
            String broker,
            String clientId,
            String topic,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            RetryStrategy retryStrategy,
            FunctionEx<T, MqttMessage> messageFn
    ) throws MqttException {
        this.logger = context.logger();
        this.topic = topic;
        this.retryStrategy = retryStrategy;
        this.messageFn = messageFn;
        this.client = client(context, broker, clientId, connectOpsFn.get());
    }

    public void publish(T item) throws MqttException, InterruptedException {
        MqttMessage message = messageFn.apply(item);
        int maxAttempts = retryStrategy.getMaxAttempts();
        IntervalFunction intervalFunction = retryStrategy.getIntervalFunction();
        if (maxAttempts < 0) {
            // try indefinitely
            maxAttempts = Integer.MAX_VALUE;
        } else if (maxAttempts < Integer.MAX_VALUE) {
            // maxAttempts=0 means try once and don't retry again
            // so we add the actual try to maxAttempts
            maxAttempts++;
        }

        MqttException lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                client.publish(topic, message).waitForCompletion(WAIT_TIMEOUT_MILLIS);
                return;
            } catch (MqttException exception) {
                lastException = exception;
                logger.warning(String.format("Exception while publishing %s to %s, current attempt: %d",
                        item, topic, attempt), exception);
                MILLISECONDS.sleep(intervalFunction.waitAfterAttempt(attempt));
            }
        }
        throw lastException;
    }

    public void close() throws MqttException {
        try {
            client.disconnect().waitForCompletion();
        } catch (MqttException exception) {
            logger.warning("Exception when disconnecting", exception);
        }
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
