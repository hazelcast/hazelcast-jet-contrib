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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistable;
import org.eclipse.paho.client.mqttv3.internal.MqttPersistentData;

import java.io.IOException;

final class MqttSerializerHook {

    public static final int MQTT_PERSISTABLE = -1883;

    private MqttSerializerHook() {
    }

    public static final class MqttPersistableHook implements SerializerHook<MqttPersistable> {

        @Override
        public Class<MqttPersistable> getSerializationType() {
            return MqttPersistable.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<MqttPersistable>() {
                @Override
                public int getTypeId() {
                    return MQTT_PERSISTABLE;
                }

                @Override
                public void write(ObjectDataOutput out, MqttPersistable persistable) throws IOException {
                    try {
                        byte[] header = new byte[persistable.getHeaderLength()];
                        int headerOffset = persistable.getHeaderOffset();
                        byte[] payload = new byte[persistable.getPayloadLength()];
                        int payloadOffset = persistable.getPayloadOffset();
                        System.arraycopy(persistable.getHeaderBytes(), headerOffset, header, 0, header.length);
                        System.arraycopy(persistable.getPayloadBytes(), payloadOffset, payload, 0, payload.length);
                        out.writeByteArray(header);
                        out.writeByteArray(payload);
                    } catch (MqttException exception) {
                        throw new IOException("Failed to write MqttPersistable", exception);
                    }
                }

                @Override
                public MqttPersistable read(ObjectDataInput in) throws IOException {
                    byte[] header = in.readByteArray();
                    byte[] payload = in.readByteArray();
                    return new MqttPersistentData(null, header, 0, header.length, payload, 0, payload.length);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
