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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.util.List;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.util.Collections.singletonList;

/**
 * todo add proper javadoc
 */
public class HiveMqttSources {

    public static StreamSource<byte[]> subscribe(String host, int port, String topic) {
        return subscribe(host, port, newUnsecureUuidString(), singletonList(Subscription.of(topic)),
                () -> Mqtt5Connect.builder().build(), Mqtt5Publish::getPayloadAsBytes);
    }

    public static <T> StreamSource<T> subscribe(
            String host,
            int port,
            String clientId,
            List<Subscription> subscriptions,
            SupplierEx<Mqtt5Connect> connectOpsFn,
            FunctionEx<Mqtt5Publish, T> mapToItemFn
    ) {
        SourceBuilder<MqttSourceContext<T>>.Stream<T> builder = SourceBuilder.stream("mqttSource",
                context -> new MqttSourceContext<>(context, host, port, clientId, subscriptions, connectOpsFn, mapToItemFn))
                .<T>fillBufferFn(MqttSourceContext::fillBuffer)
                .destroyFn(MqttSourceContext::close);
        if (subscriptions.size() > 1) {
            builder.distributed(1);
        }
        return builder.build();
    }

    static class MqttSourceContext<T> {

        private final Mqtt5BlockingClient client;
        private final Mqtt5Publishes publishes;
        private final FunctionEx<Mqtt5Publish, T> mapToItemFn;
        private final ILogger logger;

        public MqttSourceContext(
                Processor.Context context,
                String host,
                int port,
                String clientId,
                List<Subscription> subscriptions,
                SupplierEx<Mqtt5Connect> connectOpsFn,
                FunctionEx<Mqtt5Publish, T> mapToItemFn
        ) {
            this.logger = context.logger();
            this.mapToItemFn = mapToItemFn;
            subscriptions = localSubscriptions(context, subscriptions);
            if (subscriptions.size() == 0) {
                logger.info("No topics to subscribe");
                client = null;
                publishes = null;
                return;
            }
            logger.info("Subscribing to topics: " + subscriptions);
            client = client(context, host, port, clientId, connectOpsFn.get());
            publishes = client.publishes(MqttGlobalPublishFilter.SUBSCRIBED);
            subscriptions.forEach(s ->
                    client.subscribeWith().topicFilter(s.topic).qos(MqttQos.fromCode(s.qualityOfService.qos)).send());
        }

        void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
            if (publishes != null) {
                try {
                    buf.add(mapToItemFn.apply(publishes.receive()));
                } catch (InterruptedException e) {
                    logger.warning("Receive interrupted", e);
                }
            }
        }

        void close() {
            if (publishes != null) {
                publishes.close();
            }
            if (client != null) {
                client.disconnect();
            }
        }

        List<Subscription> localSubscriptions(Processor.Context context, List<Subscription> subscriptions) {
            if (subscriptions.size() == 1) {
                return subscriptions;
            }
            return Util.distributeObjects(context.totalParallelism(), subscriptions).get(context.globalProcessorIndex());
        }

        Mqtt5BlockingClient client(Processor.Context context, String host, int port, String clientId,
                                   Mqtt5Connect connectOptions) {
            clientId = clientId + "_" + context.globalProcessorIndex();
            Mqtt5BlockingClient blocking =
                    MqttClient.builder()
                              .useMqttVersion5()
                              .serverHost(host)
                              .serverPort(port)
                              .identifier(clientId)
                              .buildBlocking();

            blocking.connect(connectOptions);
            return blocking;
        }
    }

}
