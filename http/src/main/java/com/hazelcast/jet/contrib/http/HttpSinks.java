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

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating WebSocket and Server-Sent Events server sinks.
 * Clients can connect to the sinks and stream the results of the pipeline.
 * <p>
 * Server addresses can be retrieved from {@link #webSocketAddress(JetInstance)} and
 * {@link #sseAddress(JetInstance)} (JetInstance, Job)} respectively.
 */
public final class HttpSinks {

    private HttpSinks() {
    }

    /**
     * TODO
     *
     * @param <T>
     */
    public static <T> HttpSinkBuilder<T> builder() {
        return new HttpSinkBuilder<>();
    }

    /**
     * TODO
     *
     * @param <T>
     */
    public static <T> Sink<T> websocket() {
        return HttpSinks.<T>builder().buildWebsocket();
    }

    /**
     * Creates a websocket server sink with TODO
     *
     * @param path the path which websocket server accepts connections
     * @param port the offset for websocket server port to bind from the member
     *             port. For example; if Hazelcast Jet member runs on the port
     *             5701 and {@code portOffset} is set to 100, the websocket server
     *             will listen for connections on port 5801 on the same host
     *             address with the member.
     * @param <T>  type of items in the stream
     */
    public static <T> Sink<T> websocket(@Nonnull String path, int port) {
        return HttpSinks.<T>builder().path(path).port(port).buildWebsocket();
    }

    /**
     * TODO
     *
     * @param <T>
     */
    public static <T> Sink<T> sse() {
        return HttpSinks.<T>builder().buildServerSent();
    }

    /**
     * Creates a HTTP server sink with Server-Sent Events TODO
     *
     * @param path the path which server accepts connections
     * @param port the offset for server port to bind from the member
     *             port. For example; if Hazelcast Jet member runs on the port
     *             5701 and {@code portOffset} is set to 100, the server
     *             will listen for connections on port 5801 on the same host
     *             address with the member.
     * @param <T>  type of items in the stream
     */
    public static <T> Sink<T> sse(@Nonnull String path, int port) {
        return HttpSinks.<T>builder().path(path).port(port).buildServerSent();
    }

    /**
     * TODO
     *
     * @param jet
     */
    public static String webSocketAddress(JetInstance jet) {
        return webSocketAddress(jet, HttpSinkBuilder.DEFAULT_PORT, HttpSinkBuilder.DEFAULT_PATH, false);
    }

    /**
     * TODO
     *
     * @param jet
     * @param port
     * @param path
     * @param ssl
     */
    public static String webSocketAddress(JetInstance jet, int port, String path, boolean ssl) {
        return sinkAddress(jet, port, path, ssl ? "wss" : "ws");
    }

    /**
     * TODO
     *
     * @param jet
     */
    public static String sseAddress(JetInstance jet) {
        return sseAddress(jet, HttpSinkBuilder.DEFAULT_PORT, HttpSinkBuilder.DEFAULT_PATH, false);
    }

    /**
     * TODO
     *
     * @param jet
     * @param port
     * @param path
     * @param ssl
     */
    public static String sseAddress(JetInstance jet, int port, String path, boolean ssl) {
        return sinkAddress(jet, port, path, ssl ? "https" : "http");
    }

    private static String sinkAddress(JetInstance jet, int port, String path, String prefix) {
        PartitionService partitionService = jet.getHazelcastInstance().getPartitionService();
        Partition partition = partitionService.getPartition(String.valueOf(port));
        String host = partition.getOwner().getAddress().getHost();
        return prefix + "://" + host + ":" + port + path;
    }

}
