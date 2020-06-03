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
import com.hazelcast.jet.contrib.mqtt.impl.ConcurrentMemoryPersistence;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;

/**
 * Contains factory methods for Mqtt sources.
 */
public class MqttSources {

    /**
     * Convenience for {@link #subscribe(String, String, String[], SupplierEx, BiFunctionEx)}.
     * Uses a random uuid as the client id and default
     * {@link MqttConnectOptions}. Emits {@link MqttMessage} objects without
     * any conversion.
     *
     * @param broker the address of the server to connect to, specified
     *               as a URI, e.g. `tcp://localhost:1883`.
     * @param topic  the topic that the source subscribes, may include
     *               wildcards.
     */
    public static StreamSource<MqttMessage> subscribe(String broker, String topic) {
        return subscribe(broker, newUnsecureUuidString(), new String[]{topic}, MqttConnectOptions::new, (t, m) -> m);
    }

    /**
     * Creates a streaming source which connects to the given server and
     * subscribes to the specified topics. The source converts each message to
     * the desired object output using given {@code mapToItemFn} and emits to
     * downstream.
     * <p>
     * The source creates a <a href="https://www.eclipse.org/paho/clients/java/">Paho</a>
     * client with the specified client id and memory persistence {@link ConcurrentMemoryPersistence}.
     * Client connects to the given server using the specified connect options
     * and uses the maximum quality service of `2` to subscribe the topics. The
     * global processor index is appended to the specified {@code clientId} to
     * make each created client unique.
     * <p>
     * The source is not distributed if a single topic provided, otherwise
     * topics are distributed among members.
     * <p>
     * The source is not re-playable from a certain offset thus it cannot
     * participate in snapshotting and not fault tolerant. But if you set
     * {@link MqttConnectOptions#setCleanSession(boolean)} to {@code false}
     * The server will keep the published messages with quality service above
     * `0` and deliver them to the source after restart.
     *
     * @param broker       the address of the server to connect to, specified
     *                     as a URI, e.g. `tcp://localhost:1883`. Can be
     *                     overridden using {@link MqttConnectOptions#setServerURIs(String[])}.
     * @param clientId     the unique identifier for the Paho client.
     * @param topics       the topics that the source subscribes, may include wildcards.
     * @param connectOpsFn connect options supplier function.
     * @param mapToItemFn  the function that converts the messages to pipeline items.
     * @param <T>          type of the pipeline items emitted to downstream
     */
    public static <T> StreamSource<T> subscribe(
            String broker,
            String clientId,
            String[] topics,
            SupplierEx<MqttConnectOptions> connectOpsFn,
            BiFunctionEx<String, MqttMessage, T> mapToItemFn
    ) {
        SourceBuilder<MqttSourceContext<T>>.Stream<T> builder = SourceBuilder.stream("mqttSource",
                context -> new MqttSourceContext<>(broker, clientId, topics, context, connectOpsFn, mapToItemFn))
                .<T>fillBufferFn(MqttSourceContext::fillBuffer)
                .destroyFn(MqttSourceContext::close);

        if (topics.length > 1) {
            builder.distributed(1);
        }

        return builder.build();
    }

    static IMqttClient client(String broker, String clientId) throws MqttException {
        return new MqttClient(broker, clientId, new ConcurrentMemoryPersistence());
    }

    static class MqttSourceContext<T> implements MqttCallback {

        private final ILogger logger;
        private final IMqttClient client;
        private final BiFunctionEx<String, MqttMessage, T> mapToItemFn;
        private final ArrayBlockingQueue<T> queue = new ArrayBlockingQueue<>(1024);
        private final List<T> tempBuffer = new ArrayList<>(1024);

        public MqttSourceContext(
                String broker,
                String clientId,
                String[] topics,
                Processor.Context context,
                SupplierEx<MqttConnectOptions> connectOpsFn,
                BiFunctionEx<String, MqttMessage, T> mapToItemFn
        ) throws MqttException {
            this.mapToItemFn = mapToItemFn;
            this.logger = context.logger();
            topics = localTopics(context, topics);
            if (topics.length == 0) {
                logger.info("No topics to subscribe");
                client = null;
                return;
            }
            logger.info("Subscribing to topics: " + Arrays.toString(topics));

            client = client(broker, clientId + "_" + context.globalProcessorIndex());
            MqttConnectOptions options = connectOpsFn.get();
            client.connect(options);
            client.setCallback(this);

            int[] qos = new int[topics.length];
            Arrays.fill(qos, 2);

            logger.warning("is connected " + client.isConnected());
            client.subscribe(topics, qos);
        }

        void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
            queue.drainTo(tempBuffer);
            tempBuffer.forEach(buf::add);
            tempBuffer.clear();
        }

        void close() throws MqttException {
            if (client != null) {
                client.disconnect();
                client.close();
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
            logger.warning(cause);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            queue.put(mapToItemFn.apply(topic, message));
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }

        String[] localTopics(Processor.Context context, String[] topics) {
            if (topics.length == 1) {
                return topics;
            }
            List<String> topicList = Arrays.asList(topics);
            List<String> localTopicList = Util.distributeObjects(context.totalParallelism(), topicList)
                                              .get(context.globalProcessorIndex());
            return localTopicList.toArray(new String[0]);
        }
    }

}
