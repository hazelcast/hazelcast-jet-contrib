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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.util.UuidUtil;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import javax.annotation.Nonnull;

/**
 * Base class for Mqtt source and sink builder.
 * See {@link MqttSourceBuilder} and {@link MqttSinkBuilder}.
 *
 * @since 4.3
 */
abstract class AbstractMqttBuilder<T, SELF extends AbstractMqttBuilder<T, SELF>> {

    private static final String LOCAL_BROKER = "tcp://localhost:1883";

    String broker = LOCAL_BROKER;
    String clientId = UuidUtil.newUnsecureUuidString();
    String topic;
    boolean autoReconnect;
    boolean keepSession;
    String username;
    char[] password;
    SupplierEx<MqttConnectOptions> connectOptionsFn;

    /**
     * Set the address of the Mqtt broker.
     * <p>
     * For example, to connect to the local broker with a non-default port:
     * <pre>{@code
     * builder.broker("tcp://localhost:1993")
     * }</pre>
     * <p>
     * Default value is {@link #LOCAL_BROKER}.
     *
     * @param broker the address of the server to connect to, specified as a URI.
     */
    @Nonnull
    public SELF broker(@Nonnull String broker) {
        this.broker = broker;
        return (SELF) this;
    }

    /**
     * Set the id of the client. The connector appends global processor index
     * to the given id to make each created client unique.
     * <p>
     * For example, each created client will have: `the-client-0`,
     * `the-client-1`, `the-client-2`...
     * <pre>{@code
     * builder.clientId("the-client")
     * }</pre>
     * <p>
     * Default value is a randomly generated uuid.
     *
     * @param clientId the unique id of the client.
     */
    @Nonnull
    public SELF clientId(@Nonnull String clientId) {
        this.clientId = clientId;
        return (SELF) this;
    }

    /**
     * Set the topic which the connector will use.
     * See {@link MqttSourceBuilder#topic} or {@link MqttSinkBuilder#topic}.
     */
    @Nonnull
    public SELF topic(@Nonnull String topic) {
        this.topic = topic;
        return (SELF) this;
    }

    /**
     * Set that the client automatically attempt to reconnect to the broker
     * if the connection is lost.
     * <p>
     * For example:
     * <pre>{@code
     * builder.autoReconnect()
     * }</pre>
     * <p>
     * By default the client will not attempt to reconnect.
     * If {@link #connectOptionsFn(SupplierEx)} is set, this parameter is
     * ignored.
     * <p>
     * See {@link MqttConnectOptions#setAutomaticReconnect(boolean)}.
     */
    @Nonnull
    public SELF autoReconnect() {
        this.autoReconnect = true;
        return (SELF) this;
    }

    /**
     * Set that the client and broker should remember state across restarts and
     * reconnects.
     * <p>
     * For example:
     * <pre>{@code
     * builder.keepSession()
     * }</pre>
     * <p>
     * By default the client and broker will not remember the state.
     * If {@link #connectOptionsFn(SupplierEx)} is set, this parameter is
     * ignored.
     * <p>
     * See {@link MqttConnectOptions#setCleanSession(boolean)}.
     */
    @Nonnull
    public SELF keepSession() {
        this.keepSession = true;
        return (SELF) this;
    }

    /**
     * Set the username and the password to use for connection
     * <p>
     * For example:
     * <pre>{@code
     * builder.auth("the-user", "the-pass".toCharArray())
     * }</pre>
     * <p>
     * Default value of both parameters is {@code null}.
     * If {@link #connectOptionsFn(SupplierEx)} is set, these parameters are
     * ignored.
     * <p>
     * See {@link MqttConnectOptions#setUserName(String)} and
     * {@link MqttConnectOptions#setPassword(char[])}.
     *
     * @param username the username as a string.
     * @param password the password as a character array.
     */
    @Nonnull
    public SELF auth(@Nonnull String username, @Nonnull char[] password) {
        this.username = username;
        this.password = password;
        return (SELF) this;
    }

    /**
     * Set the connection options function.
     * <p>
     * For example, to set multiple broker addresses:
     * <pre>{@code
     * builder.connectOptionsFn(() -> {
     *     MqttConnectOptions connectOptions = new MqttConnectOptions();
     *     connectOptions.setServerURIs(new String[]{
     *             "tcp://the-broker-1:1883",
     *             "tcp://the-broker-2:1883",
     *             "tcp://the-broker-3:1883"
     *     });
     *     return connectOptions;
     * })
     * }</pre>
     * <p>
     * If this parameter is set, {@link #autoReconnect()}, {@link #keepSession()}
     * and {@link #auth(String, char[])} will be ignored.
     * <p>
     * See {@link MqttConnectOptions} for default values.
     *
     * @param connectOptionsFn the connection options function.
     */
    @Nonnull
    public SELF connectOptionsFn(@Nonnull SupplierEx<MqttConnectOptions> connectOptionsFn) {
        this.connectOptionsFn = connectOptionsFn;
        return (SELF) this;
    }

    SupplierEx<MqttConnectOptions> connectOpsFn() {
        if (connectOptionsFn != null) {
            return connectOptionsFn;
        }
        String user = username;
        char[] pass = password;
        boolean localAutoReconnect = autoReconnect;
        boolean cleanSession = !keepSession;
        return () -> {
            MqttConnectOptions ops = new MqttConnectOptions();
            ops.setCleanSession(cleanSession);
            ops.setAutomaticReconnect(localAutoReconnect);
            ops.setUserName(user);
            if (pass != null) {
                ops.setPassword(pass);
            }
            return ops;
        };
    }
}
