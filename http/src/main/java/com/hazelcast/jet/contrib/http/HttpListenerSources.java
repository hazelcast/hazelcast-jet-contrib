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
import com.hazelcast.com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.undertow.Undertow;
import io.undertow.server.handlers.DisallowedMethodsHandler;
import io.undertow.server.handlers.ExceptionHandler;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static io.undertow.Handlers.exceptionHandler;
import static io.undertow.Handlers.path;


/**
 * Contains factory methods for creating HTTP(S) listener sources which listens for HTTP(S)
 * requests. There are multiple variants for handling the payloads. For JSON payloads
 * you can use built-in object mapping. You can get raw string payload or provide a
 * deserializer function to deserialize the payload.
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
    public static <T> StreamSource<T> httpListener(int portOffset, @Nonnull Class<T> type) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(type, "type cannot be null");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset,
                        message -> JsonUtil.beanFrom(new String(message), type), null
                ))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTP requests which contains string payload.
     *
     * @param portOffset The offset for HTTP listener port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the HTTP listeners
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @return HTTP payload string
     */
    @Nonnull
    public static StreamSource<String> httpListener(int portOffset) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset, String::new, null))
                .<String>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTP requests. Payloads will be deserialized
     * with provided deserializer function.
     *
     * @param portOffset The offset for HTTP listener port to bind from the member
     *                   port. For example; if Hazelcast Jet member runs on the port
     *                   5701 and {@code portOffset} is set to 100, the HTTP listeners
     *                   will listen for connections on port 5801 on the same host
     *                   address with the member.
     * @return deserialized user objects
     */
    @Nonnull
    public static <T> StreamSource<T> httpListener(int portOffset, @Nonnull FunctionEx<byte[], T> deserializer) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(deserializer, "deserializer cannot be null");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset, deserializer, null))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
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
    public static <T> StreamSource<T> httpsListener(int portOffset,
                                                    @Nonnull Supplier<SSLContext> sslContextSupplier,
                                                    @Nonnull Class<T> type) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(type, "type cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset,
                        message -> JsonUtil.beanFrom(new String(message), type), sslContextSupplier.get()
                ))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTPS requests which contains string payload.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from the member
     *                           port. For example; if Hazelcast Jet member runs on the port
     *                           5701 and {@code portOffset} is set to 100, the HTTPS server
     *                           will listen for connections on port 5801 on the same host
     *                           address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener for
     *                           secure connections.
     * @return HTTP payload string
     */
    @Nonnull
    public static StreamSource<String> httpsListener(int portOffset, @Nonnull Supplier<SSLContext> sslContextSupplier) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset,
                        String::new, sslContextSupplier.get()))
                .<String>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    /**
     * Creates a source that listens for HTTPS requests.Payloads will be deserialized
     * with provided deserializer function.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from the member
     *                           port. For example; if Hazelcast Jet member runs on the port
     *                           5701 and {@code portOffset} is set to 100, the HTTPS server
     *                           will listen for connections on port 5801 on the same host
     *                           address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener for
     *                           secure connections.
     * @return deserialized user objects
     */
    @Nonnull
    public static <T> StreamSource<T> httpsListener(int portOffset, @Nonnull Supplier<SSLContext> sslContextSupplier,
                                                    @Nonnull FunctionEx<byte[], T> deserializer) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", ctx ->
                new HttpListenerSourceContext<>(ctx.jetInstance(), portOffset,
                        deserializer, sslContextSupplier.get()))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }


    private static class HttpListenerSourceContext<T> {

        private static final ILogger LOGGER = Logger.getLogger(HttpListenerSources.class);
        private static final int MAX_FILL_ELEMENTS = 256;

        private final BlockingQueue<T> queue = new ArrayBlockingQueue<>(1024);
        private final Undertow undertow;

        HttpListenerSourceContext(JetInstance jet, int portOffset,
                                  @Nonnull FunctionEx<byte[], T> deserializer, @Nullable SSLContext sslContext) {
            Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
            String host = localAddress.getHost();
            int port = localAddress.getPort() + portOffset;
            Undertow.Builder builder = Undertow.builder();
            if (sslContext != null) {
                builder.addHttpsListener(port, host, sslContext);
                LOGGER.info("Starting to listen HTTPS messages on https://" + host + ":" + port);
            } else {
                builder.addHttpListener(port, host);
                LOGGER.info("Starting to listen HTTP messages on http://" + host + ":" + port);
            }
            undertow = builder.setHandler(
                    new DisallowedMethodsHandler(path().addExactPath("/", exceptionHandler(exchange -> {
                                exchange.getRequestReceiver()
                                        .receiveFullBytes((e, message) -> {
                                            queue.offer(deserializer.apply(message));
                                            e.endExchange();
                                        }, (e, exception) -> {
                                            ExceptionUtil.sneakyThrow(exception);
                                        });
                            }
                    ).addExceptionHandler(JsonProcessingException.class, e -> {
                        LOGGER.warning("Supplied payload is not a valid JSON: " +
                                e.getAttachment(ExceptionHandler.THROWABLE).getMessage());
                        e.setStatusCode(StatusCodes.BAD_REQUEST);
                    })), Methods.PATCH, Methods.HEAD, Methods.DELETE, Methods.CONNECT, Methods.OPTIONS, Methods.TRACE)
            ).build();
            undertow.start();
        }

        void fillBuffer(SourceBuffer<T> sourceBuffer) {
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_ELEMENTS) {
                T item = queue.poll();
                sourceBuffer.add(item);
            }
        }

        void close() {
            undertow.stop();
        }
    }
}
