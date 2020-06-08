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

package com.hazelcast.jet.contrib.mqtt;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.mqtt.impl.paho.ConcurrentMemoryPersistence;
import com.hazelcast.jet.contrib.mqtt.impl.paho.SourceContext;
import com.hazelcast.jet.contrib.mqtt.impl.paho.SourceContextImpl;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.List;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.contrib.mqtt.impl.paho.NoopSourceContextImpl.noopSourceContext;
import static java.util.Collections.singletonList;

/**
 * Contains factory methods for Mqtt sources.
 */
public final class MqttSources {

    private MqttSources() {
    }

    /**
     * Convenience for {@link #subscribe(String, String, List, SupplierEx, BiFunctionEx)}.
     * Uses a random uuid as the client id and default
     * {@link MqttConnectOptions}. Emits {@link MqttMessage#getPayload()}
     * to the downstream.
     *
     * @param broker the address of the server to connect to, specified
     *               as a URI, e.g. `tcp://localhost:1883`.
     * @param topic  the topic that the source subscribes, may include
     *               wildcards.
     */
    public static StreamSource<byte[]> subscribe(String broker, String topic) {
        return subscribe(broker, newUnsecureUuidString(),
                singletonList(Subscription.of(topic)), MqttConnectOptions::new, (t, m) -> m.getPayload());
    }

    /**
     * Creates a streaming source which connects to the given broker and
     * subscribes to the specified topics. The source converts each message to
     * the desired object output using given {@code mapToItemFn} and emits to
     * downstream.
     * <p>
     * The source creates a <a href="https://www.eclipse.org/paho/clients/java/">Paho</a>
     * client with the specified client id and memory persistence {@link ConcurrentMemoryPersistence}.
     * Client connects to the given broker using the specified connect options
     * and subscribes to the given topics. The global processor index is
     * appended to the specified {@code clientId} to make each created client
     * unique.
     * <p>
     * The source is not distributed if a single topic provided, otherwise
     * topics are distributed among members. If you use wildcard in your topic
     * it is still considered by the source as a single topic.
     * <p>
     * The source is not re-playable from a certain offset thus it cannot
     * participate in snapshotting and not fault tolerant. But if you set
     * {@link MqttConnectOptions#setCleanSession(boolean)} to {@code false}
     * The broker will keep the published messages with quality service above
     * `0` and deliver them to the source after restart.
     *
     * @param broker        the address of the server to connect to, specified
     *                      as a URI, e.g. `tcp://localhost:1883`. Can be
     *                      overridden using {@link MqttConnectOptions#setServerURIs(String[])}.
     * @param clientId      the unique identifier for the Paho client.
     * @param subscriptions the subscription list.
     * @param connectOpsFn  connect options supplier function.
     * @param mapToItemFn   the function that converts the messages to pipeline items.
     * @param <T>           type of the pipeline items emitted to downstream
     */
    public static <T> StreamSource<T> subscribe(
            String broker,
            String clientId,
            List<Subscription> subscriptions,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            BiFunctionEx<String, MqttMessage, T> mapToItemFn
    ) {
        SourceBuilder<SourceContext<T>>.Stream<T> builder = SourceBuilder
                .stream("mqttSource",
                        context -> {
                            List<Subscription> localSubscriptions = localSubscriptions(context, subscriptions);
                            if (localSubscriptions.isEmpty()) {
                                return noopSourceContext(mapToItemFn);
                            }
                            return new SourceContextImpl<>(
                                    context, broker, clientId, localSubscriptions, connectOpsFn, mapToItemFn);
                        })
                .<T>fillBufferFn(SourceContext::fillBuffer)
                .destroyFn(SourceContext::close);
        if (subscriptions.size() > 1) {
            builder.distributed(1);
        }
        return builder.build();
    }

    private static List<Subscription> localSubscriptions(Processor.Context context, List<Subscription> subscriptions) {
        if (subscriptions.size() == 1) {
            return subscriptions;
        }
        return Util.distributeObjects(context.totalParallelism(), subscriptions).get(context.globalProcessorIndex());
    }
}
