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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;


/**
 * Contains factory methods for creating HTTP(S) listener sources which listens for HTTP(S)
 * requests which contains JSON payloads. The payloads will be either parsed as provided
 * class typed or {@link Map} objects and emitted to the downstream.
 */
public final class HttpListenerSources {

    private HttpListenerSources() {
    }

    /**
     * Creates a source that listens for HTTP requests which contains JSON payload.
     * Payload will be mapped to an object of the provided class type.
     *
     * @param portOffset The offset for HTTP listener port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the HTTP listeners
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @param type       Class type for the objects to be emitted. Received JSON
     *                   payloads will be mapped to objects of provided type.
     * @return mapped object from the payload of the HTTP request
     */
    @Nonnull
    public static <T> StreamSource<T> httpListener(@Nonnull int portOffset, @Nonnull Class<T> type) {
        checkPositive(portOffset, "portOffset cannot be negative");
        checkNotNull(type, "type cannot be null");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset, type))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTP requests which contains JSON payload.
     *
     * @param portOffset The offset for HTTP listener port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the HTTP listeners
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @return {@link Map} object which contains parsed JSON fields from the HTTP payload
     */
    @Nonnull
    public static StreamSource<Map<String, Object>> httpListener(@Nonnull int portOffset) {
        checkPositive(portOffset, "portOffset cannot be negative");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", ctx ->
                new HttpListenerSourceContext<Map<String, Object>>(ctx.jetInstance(), portOffset, null))
                .<Map<String, Object>>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }


    /**
     * Creates a source that listens for HTTPS requests which contains JSON payload.
     * Payload will be mapped to an object of the provided class type.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from the member
     *                           port. For example; if Hazelcast Jet member runs on the port
     *                           5701 and {@code portOffset} is set to 100, the HTTPS server
     *                           will listen for connections on port 5801 on the same host
     *                           address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener for
     *                           secure connections.
     * @param type               Class type for the objects to be emitted. Received JSON
     *                           payloads will be mapped to objects of provided type.
     * @return mapped object from the payload of the HTTPS request
     */
    @Nonnull
    public static <T> StreamSource<T> httpsListener(@Nonnull int portOffset,
                                                    @Nonnull Supplier<SSLContext> sslContextSupplier,
                                                    @Nonnull Class<T> type) {
        checkPositive(portOffset, "portOffset cannot be negative");
        checkNotNull(type, "type cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset, type, sslContextSupplier.get()))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTPS requests which contains JSON payload.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from the member
     *                           port. For example; if Hazelcast Jet member runs on the port
     *                           5701 and {@code portOffset} is set to 100, the HTTPS server
     *                           will listen for connections on port 5801 on the same host
     *                           address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener for
     *                           secure connections.
     * @return {@link Map} object which contains parsed JSON fields from the HTTPS payload
     */
    @Nonnull
    public static StreamSource<Map<String, Object>> httpsListener(@Nonnull int portOffset,
                                                                  @Nonnull Supplier<SSLContext> sslContextSupplier) {
        checkPositive(portOffset, "portOffset cannot be negative");
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", ctx ->
                new HttpListenerSourceContext<Map<String, Object>>(ctx.jetInstance(), portOffset, null,
                        sslContextSupplier.get()))
                .<Map<String, Object>>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    private static class HttpListenerSourceContext<T> {

        private static final ILogger LOGGER = Logger.getLogger(HttpListenerSources.class);
        private static final int MAX_FILL_ELEMENTS = 100;

        private final BlockingQueue<T> queue = new ArrayBlockingQueue<>(1000);
        private final ArrayList<T> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final Undertow undertow;
        private FunctionEx<String, T> mappingFunction;
        private final HttpHandler handler = exchange ->
                exchange
                        .getRequestReceiver()
                        .receiveFullString((e, message) -> {
                            queue.offer(mappingFunction.apply(message));
                            e.endExchange();
                        });

        HttpListenerSourceContext(JetInstance jet, int portOffset, @Nullable Class<T> type) {
            this(jet, portOffset, type, null);
        }

        HttpListenerSourceContext(JetInstance jet, int portOffset, @Nullable Class<T> type,
                                  @Nullable SSLContext sslContext) {
            if (type == null) {
                mappingFunction = message -> (T) JsonUtil.mapFrom(message);
            } else {
                mappingFunction = message -> JsonUtil.beanFrom(message, type);
            }
            Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
            String host = localAddress.getHost();
            int port = localAddress.getPort() + portOffset;
            if (sslContext != null) {
                undertow = Undertow.builder()
                        .addHttpsListener(port, host, sslContext)
                        .setHandler(handler)
                        .build();
                LOGGER.info("Starting to listen HTTPS messages on https://" + host + ":" + port);
            } else {
                undertow = Undertow.builder()
                        .addHttpListener(port, host)
                        .setHandler(handler)
                        .build();
                LOGGER.info("Starting to listen HTTP messages on http://" + host + ":" + port);
            }
            undertow.start();
        }

        void fillBuffer(SourceBuffer<T> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (T json : buffer) {
                sourceBuffer.add(json);
            }
            buffer.clear();
        }

        void close() {
            undertow.stop();
        }
    }
}
