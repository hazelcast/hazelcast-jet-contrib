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
import com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService;
import com.hazelcast.jet.contrib.mqtt.impl.SourceContext;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService.AT_LEAST_ONCE;


/**
 * See {@link MqttSources#builder()} and {@link AbstractMqttBuilder}.
 *
 * @param <T> the type of the pipeline item.
 * @since 4.3
 */
public final class MqttSourceBuilder<T> extends AbstractMqttBuilder<T, MqttSourceBuilder<T>> {

    private QualityOfService qualityOfService = AT_LEAST_ONCE;
    private String mapName;
    private Subscription[] subscriptions;
    private BiFunctionEx<String, MqttMessage, T> mapToItemFn;

    MqttSourceBuilder() {
    }

    /**
     * Set the topic which the source will subscribe to. The topic may include
     * wildcards. See {@link #qualityOfService(QualityOfService)}.
     * <p>
     * For example, to subscribe all the topics in the house:
     * <pre>{@code
     * builder.topic("house/#")
     * }</pre>
     * <p>
     * If {@link #subscriptions(Subscription...)} is set this parameter is
     * ignored. You should either set the topic or the subscriptions.
     *
     * @param topic the topic which the source subscribes, may include
     *              wildcards.
     */
    @Nonnull
    @Override
    public MqttSourceBuilder<T> topic(@Nonnull String topic) {
        return super.topic(topic);
    }

    /**
     * Set the quality of service value for the topic which specified via
     * {@link #topic(String)}.
     * <p>
     * For example, to subscribe to the specified topic with quality of service
     * {@link QualityOfService#EXACTLY_ONCE}:
     * <pre>{@code
     * builder.qualityOfService(QualityOfService#EXACTLY_ONCE)
     * }</pre>
     * <p>
     * If {@link #subscriptions(Subscription...)} is set this parameter is
     * ignored. Default value is {@link QualityOfService#AT_LEAST_ONCE}.
     * See {@link MqttMessage#setQos(int)}.
     *
     * @param qualityOfService the quality of service value.
     */
    @Nonnull
    public MqttSourceBuilder<T> qualityOfService(@Nonnull QualityOfService qualityOfService) {
        this.qualityOfService = qualityOfService;
        return this;
    }

    /**
     * Set the topics and their quality of service values which the source will
     * subscribe to. The topics may include wildcards.
     * <p>
     * For example, to subscribe some topics with different quality of services:
     * <pre>{@code
     * builder.subscriptions(
     *      Subscription.of("house/livingroom", QualityOfService#EXACTLY_ONCE),
     *      Subscription.of("house/diningroom", QualityOfService#AT_LEAST_ONCE),
     *      Subscription.of("house/bedroom", QualityOfService#AT_MOST_ONCE),
     * )
     * }</pre>
     * <p>
     * If this parameter is set, {@link #topic(String)} and
     * {@link #qualityOfService(QualityOfService)} are ignored. You should
     * either set the topic or the subscriptions.
     *
     * @param subscriptions the topics which the source subscribes and their
     *                      quality of service values, the topics may include
     *                      wildcards.
     */
    @Nonnull
    public MqttSourceBuilder<T> subscriptions(@Nonnull Subscription... subscriptions) {
        this.subscriptions = subscriptions;
        return this;
    }

    /**
     * Set that the client and broker should remember state across
     * restarts and reconnects. The state is persisted to the
     * {@link com.hazelcast.map.IMap} with the specified name.
     * <p>
     * For example:
     * <pre>{@code
     * builder.persistSession("mqtt-source-map")
     * }</pre>
     * <p>
     * By default the client and broker will not remember the state. If
     * this parameter is set, the source ignores the value of
     * {@link MqttConnectOptions#isCleanSession()} specified via
     * {@link #connectOptionsFn(SupplierEx)}.
     * <p>
     * See {@link MqttConnectOptions#setCleanSession(boolean)}.
     */
    @Nonnull
    public MqttSourceBuilder<T> persistSession(@Nonnull String mapName) {
        this.mapName = mapName;
        return this;
    }

    /**
     * Set the function to map {@link MqttMessage} to a pipeline item.
     * <p>
     * For example, to convert each message to a string:
     * <pre>{@code
     * builder.mapToItemFn((topic, message) -> new String(message.getPayload()))
     * }</pre>
     * <p>
     * If not set, the payload({@code byte[]}) of the messages will be emitted
     * to the downstream.
     *
     * @param mapToItemFn the function which maps messages to pipeline items
     * @param <T_NEW>     the type of the pipeline item
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> MqttSourceBuilder<T_NEW> mapToItemFn(@Nonnull BiFunctionEx<String, MqttMessage, T_NEW> mapToItemFn) {
        MqttSourceBuilder<T_NEW> newThis = (MqttSourceBuilder<T_NEW>) this;
        newThis.mapToItemFn = mapToItemFn;
        return newThis;
    }

    /**
     * Build a Mqtt {@link StreamSource} with the supplied parameters.
     */
    @Nonnull
    public StreamSource<T> build() {
        String localBroker = broker;
        String localClientId = clientId;
        String localMapName = mapName;
        List<Subscription> subscriptionList = subscriptionList();
        BiFunctionEx<String, MqttMessage, T> localMapToItemFn = mapFn();
        SupplierEx<MqttConnectOptions> connectOpsFn = connectOpsFn();

        SourceBuilder<SourceContext<T>>.Stream<T> builder = SourceBuilder
                .stream("mqttSource", context -> new SourceContext<>(context, localBroker, localClientId,
                        localMapName, subscriptionList, connectOpsFn, localMapToItemFn))
                .<T>fillBufferFn(SourceContext::fillBuffer)
                .destroyFn(SourceContext::close);

        return builder.build();
    }

    private List<Subscription> subscriptionList() {
        if (subscriptions == null && topic == null) {
            throw new IllegalArgumentException("Topic or subscriptions must be set");
        }
        if (subscriptions != null) {
            return Arrays.asList(subscriptions);
        }
        return Collections.singletonList(Subscription.of(topic, qualityOfService));
    }

    private BiFunctionEx<String, MqttMessage, T> mapFn() {
        if (mapToItemFn != null) {
            return mapToItemFn;
        }
        return ((t, m) -> (T) m.getPayload());
    }

}
