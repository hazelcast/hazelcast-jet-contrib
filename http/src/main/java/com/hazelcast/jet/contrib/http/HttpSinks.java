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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.sse.ServerSentEventHandler;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.jet.contrib.http.HttpSinks.SinkNoActiveClientPolicy.DROP;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;
import static io.undertow.Handlers.path;

/**
 * Contains factory methods for creating WebSocket and Server-Sent Events server sinks.
 * Clients can connect to the sinks and stream the results of the pipeline.
 *
 * Server addresses can be retrieved from {@link #getWebSocketAddress(JetInstance, Job)} and
 * {@link #getSseAddress(JetInstance, Job)} (JetInstance, Job)} respectively.
 */
public final class HttpSinks {

    private HttpSinks() {
    }

    /**
     * Creates a websocket server sink with default {@link SinkNoActiveClientPolicy#DROP}
     * policy where clients connect and stream results of the pipeline.
     *
     * @param path       the path which websocket server accepts connections
     * @param portOffset the offset for websocket server port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the websocket server
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @param <T>        type of items in the stream
     */
    public static <T> Sink<T> websocket(@Nonnull String path, int portOffset) {
        return websocket(path, portOffset, DROP);
    }

    /**
     * Creates a websocket server sink where clients connect and stream results of the pipeline.
     *
     * @param path       the path which websocket server accepts connections
     * @param portOffset the offset for websocket server port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the websocket server
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @param policy     the policy which defines behavior of the sink when there
     *                   are no connected clients.
     * @param <T>        type of items in the stream
     */
    public static <T> Sink<T> websocket(@Nonnull String path, int portOffset,
                                        @Nonnull SinkNoActiveClientPolicy policy) {
        SupplierEx<Processor> supplier = SinkProcessors.writeBufferedP(
                ctx -> new SinkWebsocketContext<>(ctx, path, portOffset, policy),
                SinkWebsocketContext::receive,
                SinkWebsocketContext::flush,
                SinkWebsocketContext::close
        );
        String sinkName = "websocket";
        ProcessorMetaSupplier metaSupplier = forceTotalParallelismOne(ProcessorSupplier.of(supplier), sinkName);
        return new SinkImpl<>(sinkName, metaSupplier, TOTAL_PARALLELISM_ONE);
    }

    /**
     * Creates a HTTP server sink with Server-Sent Events using default {@link SinkNoActiveClientPolicy#DROP}
     * policy where clients connect and stream results of the pipeline.
     *
     * @param path       the path which server accepts connections
     * @param portOffset the offset for server port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the server
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @param <T>        type of items in the stream
     */
    public static <T> Sink<T> sse(@Nonnull String path, int portOffset) {
        return sse(path, portOffset, DROP);
    }

    /**
     * Creates a HTTP server sink with Server-Sent Events.
     *
     * @param path       the path which server accepts connections
     * @param portOffset the offset for server port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the server
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @param policy     the policy which defines behavior of the sink when there
     *                   are no connected clients.
     * @param <T>        type of items in the stream
     */
    public static <T> Sink<T> sse(@Nonnull String path, int portOffset,
                                  @Nonnull SinkNoActiveClientPolicy policy) {
        SupplierEx<Processor> supplier = SinkProcessors.writeBufferedP(
                ctx -> new SinkSSEContext<>(ctx, path, portOffset, policy),
                SinkSSEContext::receive,
                SinkSSEContext::flush,
                SinkSSEContext::close
        );
        String sinkName = "sse";
        ProcessorMetaSupplier metaSupplier = forceTotalParallelismOne(ProcessorSupplier.of(supplier), sinkName);
        return new SinkImpl<>(sinkName, metaSupplier, TOTAL_PARALLELISM_ONE);
    }

    /**
     * Returns the websocket connection string for the job if exists
     */
    public static String getWebSocketAddress(JetInstance jet, Job job) {
        String observableName = getWebSocketObservableNameByJobId(job.getId());
        return observe(jet, observableName);

    }

    /**
     * Returns the SSE connection string for the job if exists
     */
    public static String getSseAddress(JetInstance jet, Job job) {
        String observableName = getSseObservableNameByJobId(job.getId());
        return observe(jet, observableName);
    }

    /**
     * Defines what to do for the items received in the sink when there are no clients connected.
     */
    public enum SinkNoActiveClientPolicy {
        /**
         * Accumulates items in-memory until a client connects to the sink. When a client connects,
         * the sink will publish all the accumulated items to the client and continue operating.
         * <p>
         * Beware the fact that the sink accumulates items in-memory with an unbounded fashion.
         * This might lead to an out of memory error when the pipeline creates items
         * and no client is consuming them.
         */
        ACCUMULATE_AND_PUBLISH_WHEN_CONNECTED,

        /**
         * Sink will drop the events when there is no connected clients. This is the default behavior.
         */
        DROP;
    }

    private static class SinkWebsocketContext<T> {

        private static final ILogger LOGGER = Logger.getLogger(HttpSinks.class);
        private static final int MAX_FILL_MESSAGES = 1024;

        private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        private final Undertow undertow;
        private final SinkNoActiveClientPolicy policy;
        private WebSocketChannel channel;
        private final Ringbuffer<String> ringbuffer;

        SinkWebsocketContext(Processor.Context ctx, String path, int portOffset, SinkNoActiveClientPolicy policy) {
            this.policy = policy;
            Address localAddress = ctx.jetInstance().getHazelcastInstance().getCluster().getLocalMember().getAddress();
            String host = localAddress.getHost();
            int port = localAddress.getPort() + portOffset;
            undertow = Undertow
                    .builder()
                    .addHttpListener(port, host)
                    .setHandler(path().addPrefixPath(path,
                            Handlers.websocket((exchange, channel) -> {
                                this.channel = channel;
                                flush();
                            })))
                    .build();
            undertow.start();
            ringbuffer = ctx
                    .jetInstance()
                    .getHazelcastInstance()
                    .getRingbuffer(ObservableImpl.ringbufferName(getWebSocketObservableNameByJobId(ctx.jobId())));
            ringbuffer.add("ws://" + host + ":" + port + path);
            LOGGER.info("Starting to publish messages on ws://" + host + ":" + port + path);
        }

        void receive(T item) {
            if (hasNoConnectedClients() && policy == DROP) {
                return;
            }
            queue.offer(item.toString());
        }

        void flush() {
            if (hasNoConnectedClients()) {
                return;
            }
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_MESSAGES) {
                String message = queue.poll();
                channel.getPeerConnections().forEach(channel -> WebSockets.sendText(message, channel, null));
            }
        }

        private boolean hasNoConnectedClients() {
            return channel == null || channel.getPeerConnections().isEmpty();
        }

        void close() {
            undertow.stop();
            ringbuffer.destroy();
        }
    }

    private static class SinkSSEContext<T> {

        private static final ILogger LOGGER = Logger.getLogger(HttpSinks.class);
        private static final int MAX_FILL_MESSAGES = 1024;

        private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        private final Undertow undertow;
        private final SinkNoActiveClientPolicy policy;
        private final ServerSentEventHandler handler;
        private final Ringbuffer<String> ringbuffer;

        SinkSSEContext(Processor.Context ctx, String path, int portOffset, SinkNoActiveClientPolicy policy) {
            this.policy = policy;
            Address localAddress = ctx.jetInstance().getHazelcastInstance().getCluster().getLocalMember().getAddress();
            String host = localAddress.getHost();
            int port = localAddress.getPort() + portOffset;
            handler = Handlers.serverSentEvents((connection, lastEventId) -> {
                flush();
            });
            undertow = Undertow
                    .builder()
                    .addHttpListener(port, host)
                    .setHandler(path().addPrefixPath(path, handler))
                    .build();
            undertow.start();
            ringbuffer = ctx
                    .jetInstance()
                    .getHazelcastInstance()
                    .getRingbuffer(ObservableImpl.ringbufferName(getSseObservableNameByJobId(ctx.jobId())));
            ringbuffer.add("http://" + host + ":" + port + path);
            LOGGER.info("Starting to publish messages on http://" + host + ":" + port + path);
        }

        void receive(T item) {
            if (hasNoConnectedClients() && policy == DROP) {
                return;
            }
            queue.offer(item.toString());
        }

        void flush() {
            if (hasNoConnectedClients()) {
                return;
            }
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_MESSAGES) {
                String message = queue.poll();
                handler.getConnections().forEach(connection -> connection.send(message));
            }
        }

        private boolean hasNoConnectedClients() {
            return handler == null || handler.getConnections().isEmpty();
        }

        void close() {
            undertow.stop();
            ringbuffer.destroy();
        }
    }

    private static String observe(JetInstance jet, String observableName) {
        long size = jet.getHazelcastInstance().getRingbuffer(ObservableImpl.ringbufferName(observableName)).size();
        return size > 0 ? jet.<String>getObservable(observableName).iterator().next() : null;
    }

    private static String getWebSocketObservableNameByJobId(long id) {
        return Util.idToString(id) + "-websocket-sink";
    }

    private static String getSseObservableNameByJobId(long id) {
        return Util.idToString(id) + "-sse-sink";
    }

}
