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

import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;

abstract class AbstractCallback implements MqttCallbackExtended {

    final ILogger logger;
    IMqttClient client;

    AbstractCallback(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.warning(cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        throw new UnsupportedOperationException();
    }

    public void setClient(IMqttClient client) {
        this.client = client;
    }
}
