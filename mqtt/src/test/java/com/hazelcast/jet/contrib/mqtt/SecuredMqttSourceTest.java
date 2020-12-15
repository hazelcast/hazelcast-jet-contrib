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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.junit.Rule;
import org.junit.Test;

import static com.hazelcast.jet.contrib.mqtt.SecuredMosquittoContainer.PASSWORD;
import static com.hazelcast.jet.contrib.mqtt.SecuredMosquittoContainer.USERNAME;
import static com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService.EXACTLY_ONCE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SecuredMqttSourceTest extends AbstractMqttSourceTest {

    @Rule
    public MosquittoContainer mosquittoContainer = new SecuredMosquittoContainer();

    @Override
    public MosquittoContainer getContainter() {
        return mosquittoContainer;
    }

    @Override
    protected void additionalOptionsForClient(MqttConnectOptions options) {
        options.setUserName(USERNAME);
        options.setPassword(PASSWORD.toCharArray());
    }

    @Override
    protected void additionalOptionsForSource(MqttSourceBuilder<byte[]> builder) {
        builder.auth(USERNAME, PASSWORD.toCharArray());
    }

    @Test
    public void accessWithoutPassword() throws MqttException {
        Pipeline p = Pipeline.create();
        MqttSourceBuilder<byte[]> builder = MqttSources.builder()
                .broker(broker)
                .topic("topic1")
                .qualityOfService(EXACTLY_ONCE);
        StreamSource<byte[]> source = builder.build();

        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());

        assertThatThrownBy(() -> instance().newJob(p).join())
                .hasCauseInstanceOf(JetException.class)
                .hasRootCauseInstanceOf(MqttSecurityException.class)
                .hasMessageContaining("Not authorized to connect");
    }

    @Test
    public void accessWithWrongPassword() throws MqttException {
        Pipeline p = Pipeline.create();
        MqttSourceBuilder<byte[]> builder = MqttSources.builder()
                .broker(broker)
                .auth(USERNAME, "wrongPassword".toCharArray())
                .topic("topic1")
                .qualityOfService(EXACTLY_ONCE);
        StreamSource<byte[]> source = builder.build();

        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());

        assertThatThrownBy(() -> instance().newJob(p).join())
                .hasCauseInstanceOf(JetException.class)
                .hasRootCauseInstanceOf(MqttSecurityException.class)
                .hasMessageContaining("Not authorized to connect");
    }
}
