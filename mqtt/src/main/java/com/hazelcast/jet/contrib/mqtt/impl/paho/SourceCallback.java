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

package com.hazelcast.jet.contrib.mqtt.impl.paho;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * An MQTT client callback which stores the items, after applying the
 * given mapper, in a {@link BlockingQueue}as the messages arrive. The
 * items are consumed by draining the queue to a temporary buffer.
 */
public class SourceCallback<T> extends AbstractCallback {

    private static final int CAPACITY = 1024;

    private final BlockingQueue<T> queue;
    private final List<T> tempBuffer;
    private final BiFunctionEx<String, MqttMessage, T> mapToItemFn;

    public SourceCallback(ILogger logger, BiFunctionEx<String, MqttMessage, T> mapToItemFn) {
        super(logger);
        this.mapToItemFn = mapToItemFn;
        this.queue = new ArrayBlockingQueue<>(CAPACITY);
        this.tempBuffer = new ArrayList<>(CAPACITY);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        queue.offer(mapToItemFn.apply(topic, message));
    }

    public void consume(Consumer<T> consumer) {
        queue.drainTo(tempBuffer);
        tempBuffer.forEach(consumer);
        tempBuffer.clear();
    }

}
