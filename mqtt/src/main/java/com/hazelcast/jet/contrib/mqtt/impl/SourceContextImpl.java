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
import com.hazelcast.jet.pipeline.SourceBuilder;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.List;

public class SourceContextImpl<T> implements SourceContext<T> {

    private final IMqttClient client;
    private final SourceCallback<T> callback;

    public SourceContextImpl(
            Processor.Context context,
            String broker,
            String clientId,
            List<Subscription> subscriptions,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            BiFunctionEx<String, MqttMessage, T> mapToItemFn
    ) throws MqttException {
        context.logger().info("Subscribing to topics: " + subscriptions);
        callback = new SourceCallback<>(context.logger(), mapToItemFn);
        client = client(context, broker, clientId, connectOpsFn.get());
        String[] topics = subscriptions.stream().map(Subscription::getTopic).toArray(String[]::new);
        int[] qos = subscriptions.stream().mapToInt(s -> s.getQualityOfService().getQos()).toArray();
        client.subscribe(topics, qos);
    }

    @Override
    public void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
        callback.consume(buf::add);
    }

    @Override
    public void close() throws MqttException {
        client.disconnect();
    }

    IMqttClient client(Processor.Context context, String broker, String clientId,
                       MqttConnectOptions connectOptions) throws MqttException {
        clientId = clientId + "-" + context.globalProcessorIndex();
        MqttClient client = new MqttClient(broker, clientId, new ConcurrentMemoryPersistence());
        client.setCallback(callback);
        client.connect(connectOptions);
        return client;
    }
}
