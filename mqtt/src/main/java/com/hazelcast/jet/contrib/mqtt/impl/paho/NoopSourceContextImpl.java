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
import com.hazelcast.jet.pipeline.SourceBuilder;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * A noop source context, typically used when the source is distributed
 * and the subscription size is smaller than total parallelism.
 */
public final class NoopSourceContextImpl<T> implements SourceContext<T> {

    private NoopSourceContextImpl() {
    }

    @Override
    public void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
    }

    @Override
    public void close() {
    }

    public static <T> NoopSourceContextImpl<T> noopSourceContext(BiFunctionEx<String, MqttMessage, T> ignored) {
        return new NoopSourceContextImpl<>();
    }
}
