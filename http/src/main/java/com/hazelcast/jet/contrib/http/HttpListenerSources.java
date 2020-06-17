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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.http.impl.HttpListenerSourceContext;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;


/**
 * Contains factory methods for creating HTTP(S) listener sources which
 * listens for HTTP(S) requests. There are multiple variants for
 * handling the payloads. For JSON payloads you can use built-in object
 * mapping. You can get raw string payload or provide a
 * {@code mapToItemFn} function to deserialize the payload.
 */
public final class HttpListenerSources {

    private HttpListenerSources() {
    }

    /**
     * Creates a source that listens for HTTP requests which contains JSON
     * payload. Source maps the payload to an object of the given class type.
     *
     * @param portOffset The offset for HTTP listener port to bind from the
     *                   member port. For example; if Hazelcast Jet member runs
     *                   on the port 5701 and {@code portOffset} is set to 100,
     *                   the HTTP listeners will listen for connections on port
     *                   5801 on the same host address with the member.
     * @param type       Class type for the objects to be emitted. Received
     *                   JSON payloads will be mapped to objects of provided
     *                   type.
     */
    @Nonnull
    public static <T> StreamSource<T> httpListener(int portOffset, @Nonnull Class<T> type) {
        checkNotNull(type, "type cannot be null");

        return listener("http-listener(base port +" + portOffset + ")", portOffset, null,
                message -> JsonUtil.beanFrom(new String(message), type));
    }

    /**
     * Creates a source that listens for HTTP requests which contains string
     * payload.
     *
     * @param portOffset The offset for HTTP listener port to bind from the
     *                   member port. For example; if Hazelcast Jet member runs
     *                   on the port 5701 and {@code portOffset} is set to 100,
     *                   the HTTP listeners will listen for connections on port
     *                   5801 on the same host address with the member.
     */
    @Nonnull
    public static StreamSource<String> httpListener(int portOffset) {
        return listener("http-listener(base port +" + portOffset + ")", portOffset, null, String::new);
    }

    /**
     * Creates a source that listens for HTTP requests. Source converts the
     * payload to the pipeline item using given {@code mapToItemFn} function.
     *
     * @param portOffset The offset for HTTP listener port to bind from the
     *                   member port. For example; if Hazelcast Jet member runs
     *                   on the port 5701 and {@code portOffset} is set to 100,
     *                   the HTTP listeners will listen for connections on port
     *                   5801 on the same host address with the member.
     */
    @Nonnull
    public static <T> StreamSource<T> httpListener(int portOffset, @Nonnull FunctionEx<byte[], T> mapToItemFn) {
        return listener("http-listener(base port +" + portOffset + ")", portOffset,
                null, mapToItemFn);
    }


    /**
     * Creates a source that listens for HTTPS requests which contains JSON
     * payload. Source maps the payload to an object of the given class type.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from
     *                           the member port. For example; if Hazelcast Jet
     *                           member runs on the port 5701 and
     *                           {@code portOffset} is set to 100, the HTTPS
     *                           server will listen for connections on port 5801
     *                           on the same host address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener
     *                           for secure connections.
     * @param type               Class type for the objects to be emitted.
     *                           Received JSON payloads will be mapped to
     *                           objects of provided type.
     */
    @Nonnull
    public static <T> StreamSource<T> httpsListener(
            int portOffset,
            @Nonnull SupplierEx<SSLContext> sslContextSupplier,
            @Nonnull Class<T> type
    ) {
        checkNotNull(type, "type cannot be null");
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return listener("https-listener(base port + " + portOffset + ")", portOffset, sslContextSupplier,
                message -> JsonUtil.beanFrom(new String(message), type));
    }

    /**
     * Creates a source that listens for HTTPS requests which contains string
     * payload.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from
     *                           the member port. For example; if Hazelcast Jet
     *                           member runs on the port 5701 and
     *                           {@code portOffset} is set to 100, the HTTPS
     *                           server will listen for connections on port 5801
     *                           on the same host address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener
     *                           for secure connections.
     */
    @Nonnull
    public static StreamSource<String> httpsListener(int portOffset, @Nonnull SupplierEx<SSLContext> sslContextSupplier) {
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return listener("https-listener(base port + " + portOffset + ")",
                portOffset, sslContextSupplier, String::new);
    }

    /**
     * Creates a source that listens for HTTPS requests. Source converts the
     * payload to the pipeline item using given {@code mapToItemFn} function.
     *
     * @param portOffset         The offset for HTTPS listener port to bind from
     *                           the member port. For example; if Hazelcast Jet
     *                           member runs on the port 5701 and
     *                           {@code portOffset} is set to 100, the HTTPS
     *                           server will listen for connections on port 5801
     *                           on the same host address with the member.
     * @param sslContextSupplier the function to create {@link SSLContext} which
     *                           used to initialize underlying HTTPS listener
     *                           for secure connections.
     */
    @Nonnull
    public static <T> StreamSource<T> httpsListener(
            int portOffset,
            @Nonnull SupplierEx<SSLContext> sslContextSupplier,
            @Nonnull FunctionEx<byte[], T> mapToItemFn
    ) {
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return listener("https-listener(base port + " + portOffset + ")",
                portOffset, sslContextSupplier, mapToItemFn);
    }

    private static <T> StreamSource<T> listener(
            String name,
            int portOffset,
            @Nullable SupplierEx<SSLContext> sslContextSupplier,
            @Nonnull FunctionEx<byte[], T> mapToItemFn
    ) {
        checkPositive(portOffset, "portOffset cannot be zero or negative");
        checkNotNull(mapToItemFn, "mapToItemFn cannot be null");

        return SourceBuilder.stream(name,
                ctx -> new HttpListenerSourceContext<>(ctx, portOffset, sslContextSupplier, mapToItemFn))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }


}
