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

import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttPersistable;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A variant of {@link MemoryPersistence} which uses
 * {@link ConcurrentMap} instead of {@link Hashtable}.
 */
public class ConcurrentMemoryPersistence implements MqttClientPersistence {

    ConcurrentMap<String, MqttPersistable> data;

    @Override
    public void open(String clientId, String serverURI) {
        data = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        if (data != null) {
            data.clear();
        }
    }

    @Override
    public void put(String key, MqttPersistable persistable) throws MqttPersistenceException {
        checkIsOpen();
        data.put(key, persistable);
    }

    @Override
    public MqttPersistable get(String key) throws MqttPersistenceException {
        checkIsOpen();
        return data.get(key);
    }

    @Override
    public void remove(String key) throws MqttPersistenceException {
        checkIsOpen();
        data.remove(key);
    }

    @Override
    public Enumeration<String> keys() throws MqttPersistenceException {
        checkIsOpen();
        return Collections.enumeration(data.keySet());
    }

    @Override
    public void clear() throws MqttPersistenceException {
        checkIsOpen();
        data.clear();
    }

    @Override
    public boolean containsKey(String key) throws MqttPersistenceException {
        checkIsOpen();
        return data.containsKey(key);
    }

    /**
     * Checks whether the persistence has been opened.
     *
     * @throws MqttPersistenceException if the persistence has not been opened.
     */
    private void checkIsOpen() throws MqttPersistenceException {
        if (data == null) {
            throw new MqttPersistenceException();
        }
    }
}
