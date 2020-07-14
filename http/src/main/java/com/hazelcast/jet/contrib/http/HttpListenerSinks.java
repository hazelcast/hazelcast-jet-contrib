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

package com.hazelcast.jet.contrib.http;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import io.undertow.Undertow;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating WebSocket and Server-Sent
 * Events sinks. Clients can connect to the sinks and stream the
 * results of the pipeline.
 * <p>
 * Server addresses can be retrieved from
 * {@link #webSocketAddress(JetInstance, int, String, boolean)} and
 * {@link #sseAddress(JetInstance, int, String, boolean)}} respectively.
 */
public final class HttpListenerSinks {

    private HttpListenerSinks() {
    }

    /**
     * Return a builder object which offers a step-by-step fluent API to build
     * a custom HTTP(s) Listener {@link Sink sink} for the Pipeline API.
     * <p>
     * The sink is not distributed, it creates an {@link Undertow} server at
     * one of the members and send items to the connected clients at specified
     * port. If user provides an ssl context, clients should connect with a
     * secure connection.
     */
    @Nonnull
    public static <T> HttpListenerSinkBuilder<T> builder() {
        return new HttpListenerSinkBuilder<>();
    }

    /**
     * Create a Websocket sink which sends items to the connected clients at
     * {@link HttpListenerSinkBuilder#DEFAULT_PORT} and {@link HttpListenerSinkBuilder#DEFAULT_PATH}
     * by converting each item to string using {@link Object#toString()}. Sink
     * does not use secure connections and does not accumulate items if there
     * is no connected client.
     * <p>
     * See {@link #builder()}
     */
    @Nonnull
    public static <T> Sink<T> websocket() {
        return HttpListenerSinks.<T>builder().buildWebsocket();
    }

    /**
     * Create a Websocket sink which sends items to the connected clients at
     * specified {@code port} and {@code path} by converting each item to
     * string using {@link Object#toString()}. Sink does not use secure
     * connections and does not accumulate items if there is no connected
     * client.
     * <p>
     * See {@link #builder()}
     *
     * @param path the path which websocket server accepts connections
     * @param port the port which websocket server to bind.
     */
    @Nonnull
    public static <T> Sink<T> websocket(@Nonnull String path, int port) {
        return HttpListenerSinks.<T>builder().path(path).port(port).buildWebsocket();
    }

    /**
     * Create a Server-sent Event sink which sends items to the connected
     * clients at {@link HttpListenerSinkBuilder#DEFAULT_PORT} and
     * {@link HttpListenerSinkBuilder#DEFAULT_PATH} by converting each item to string
     * using {@link Object#toString()}. Sink does not use secure connections
     * and does not accumulate items if there is no connected client.
     * <p>
     * See {@link #builder()}
     */
    @Nonnull
    public static <T> Sink<T> sse() {
        return HttpListenerSinks.<T>builder().buildServerSent();
    }

    /**
     * Create a Server-sent Event sink which sends items to the connected
     * clients at specified {@code port} and {@code path} by converting each
     * item to string using {@link Object#toString()}. Sink does not use secure
     * connections and does not accumulate items if there is no connected
     * client.
     * <p>
     * See {@link #builder()}
     *
     * @param path the path which Server-sent Event server accepts connections
     * @param port the port which Server-sent Event server to bind.
     */
    @Nonnull
    public static <T> Sink<T> sse(@Nonnull String path, int port) {
        return HttpListenerSinks.<T>builder().path(path).port(port).buildServerSent();
    }

    /**
     * Convenience for {@link #webSocketAddress(JetInstance, int, String, boolean)}.
     * Uses {@link HttpListenerSinkBuilder#DEFAULT_PORT},
     * {@link HttpListenerSinkBuilder#DEFAULT_PATH} and non secure connection.
     */
    public static String webSocketAddress(JetInstance jet) {
        return webSocketAddress(jet, HttpListenerSinkBuilder.DEFAULT_PORT, HttpListenerSinkBuilder.DEFAULT_PATH, false);
    }

    /**
     * Return the websocket connection string for the given parameters.
     *
     * @param jet    the Jet instance
     * @param port   the port specified for the sink
     * @param path   the path specified for the sink
     * @param secure true if an ssl context specified for the sink.
     */
    public static String webSocketAddress(JetInstance jet, int port, String path, boolean secure) {
        return sinkAddress(jet, port, path, secure ? "wss" : "ws");
    }

    /**
     * Convenience for {@link #sseAddress(JetInstance, int, String, boolean)}.
     * Uses {@link HttpListenerSinkBuilder#DEFAULT_PORT},
     * {@link HttpListenerSinkBuilder#DEFAULT_PATH} and non secure connection.
     */
    public static String sseAddress(JetInstance jet) {
        return sseAddress(jet, HttpListenerSinkBuilder.DEFAULT_PORT, HttpListenerSinkBuilder.DEFAULT_PATH, false);
    }

    /**
     * Return the server-sent event server connection string for the given
     * parameters.
     *
     * @param jet    the Jet instance
     * @param port   the port specified for the sink
     * @param path   the path specified for the sink
     * @param secure true if an ssl context specified for the sink.
     */
    public static String sseAddress(JetInstance jet, int port, String path, boolean secure) {
        return sinkAddress(jet, port, path, secure ? "https" : "http");
    }

    private static String sinkAddress(JetInstance jet, int port, String path, String prefix) {
        PartitionService partitionService = jet.getHazelcastInstance().getPartitionService();
        Partition partition = partitionService.getPartition(String.valueOf(port));
        String host = partition.getOwner().getAddress().getHost();
        return prefix + "://" + host + ":" + port + path;
    }

}
