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

import com.hazelcast.map.IMap;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttPersistable;

import java.util.Collections;
import java.util.Enumeration;

public class IMapClientPersistence implements MqttClientPersistence {

    private final IMap<String, MqttPersistable> map;

    public IMapClientPersistence(IMap<String, MqttPersistable> map) {
        this.map = map;
    }

    @Override
    public void open(String clientId, String serverURI) {
    }

    @Override
    public void close() {
    }

    @Override
    public void put(String key, MqttPersistable persistable) {
        map.set(key, persistable);
    }

    @Override
    public MqttPersistable get(String key) {
        return map.get(key);
    }

    @Override
    public void remove(String key) {
        map.delete(key);
    }

    @Override
    public Enumeration keys() {
        return Collections.enumeration(map.keySet());
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }
}
