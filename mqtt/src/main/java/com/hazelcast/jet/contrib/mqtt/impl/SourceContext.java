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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.mqtt.Subscription;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class SourceContext<T> {

    private static final int CAPACITY = 1024;

    private final ILogger logger;
    private final IMqttClient client;
    private final List<Subscription> subscriptions;
    private final SourceCallback callback;

    public SourceContext(
            Processor.Context context,
            String broker,
            String clientId,
            String mapName,
            List<Subscription> subscriptions,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            BiFunctionEx<String, MqttMessage, T> mapToItemFn
    ) throws MqttException {
        logger = context.logger();
        this.subscriptions = subscriptions;
        callback = new SourceCallback(mapToItemFn);
        client = new MqttClient(broker, clientId, persistence(context, mapName));
        client.setCallback(callback);
        MqttConnectOptions options = connectOpsFn.get();
        if (mapName != null) {
            options.setCleanSession(false);
        }
        client.connect(options);
    }

    public void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
        callback.consume(buf::add);
    }

    public void close() throws MqttException {
        client.disconnect();
        client.close();
    }

    private static MqttClientPersistence persistence(Processor.Context context, String mapName) {
        if (mapName == null) {
            return new ConcurrentMemoryPersistence();
        }
        return new IMapClientPersistence(context.jetInstance().getMap(mapName));
    }

    class SourceCallback implements MqttCallbackExtended {

        private final BlockingQueue<T> queue;
        private final List<T> tempBuffer;
        private final BiFunctionEx<String, MqttMessage, T> mapToItemFn;

        SourceCallback(BiFunctionEx<String, MqttMessage, T> mapToItemFn) {
            this.queue = new ArrayBlockingQueue<>(CAPACITY);
            this.tempBuffer = new ArrayList<>(CAPACITY);
            this.mapToItemFn = mapToItemFn;
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            logger.info("Connection(reconnect=" + reconnect + ") to " + serverURI +
                    " complete. Subscribing to topics: " + subscriptions);
            String[] topics = subscriptions.stream().map(Subscription::getTopic).toArray(String[]::new);
            int[] qos = subscriptions.stream().mapToInt(s -> s.getQualityOfService().getQos()).toArray();
            try {
                client.subscribe(topics, qos);
            } catch (MqttException e) {
                logger.severe("Exception during subscribing, topics: " + subscriptions, e);
                throw ExceptionUtil.rethrow(e);
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
            logger.warning(cause);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws InterruptedException {
            queue.put(mapToItemFn.apply(topic, message));
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }

        public void consume(Consumer<T> consumer) {
            queue.drainTo(tempBuffer);
            tempBuffer.forEach(consumer);
            tempBuffer.clear();
        }
    }
}
