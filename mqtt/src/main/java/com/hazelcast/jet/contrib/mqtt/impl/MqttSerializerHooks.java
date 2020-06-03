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
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;

/**
 * Hazelcast serializer hooks for Mqtt data objects.
 */
public class MqttSerializerHooks {

    public static final int MESSAGE = -375;

    public static final class MessageHook implements SerializerHook<MqttMessage> {

        @Override
        public Class<MqttMessage> getSerializationType() {
            return MqttMessage.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<MqttMessage>() {
                @Override
                public int getTypeId() {
                    return MESSAGE;
                }

                @Override
                public void write(ObjectDataOutput out, MqttMessage message) throws IOException {
                    out.writeByteArray(message.getPayload());
                    out.writeInt(message.getId());
                    out.writeInt(message.getQos());
                    out.writeBoolean(message.isRetained());
                }

                @Override
                public MqttMessage read(ObjectDataInput in) throws IOException {
                    MqttMessage message = new MqttMessage(in.readByteArray());
                    message.setId(in.readInt());
                    message.setQos(in.readInt());
                    message.setRetained(in.readBoolean());
                    return message;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

}
