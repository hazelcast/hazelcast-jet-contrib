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
import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * See {@link HttpListenerSources#builder()}.
 *
 * @param <T> the type of the pipeline item.
 */
public class HttpListenerSourceBuilder<T> {

    /**
     * Default port for HTTP(s) listener
     */
    public static final int DEFAULT_PORT = 8080;

    private static final int PORT_MAX = 65535;

    private int port = DEFAULT_PORT;
    private SupplierEx<SSLContext> sslContextFn;
    private FunctionEx<byte[], T> mapToItemFn;
    private Class<T> type;

    HttpListenerSourceBuilder() {
    }

    /**
     * Set the port which the source binds and listens.
     * <p>
     * For example to bind to port `5802`:
     * <pre>{@code
     * builder.port(5802)
     * }</pre>
     * <p>
     * Default value is {@link #DEFAULT_PORT} {@code 5801}.
     *
     * @param port the port which the source binds and listens.
     */
    @Nonnull
    public HttpListenerSourceBuilder<T> port(int port) {
        if (port < 0 || port > PORT_MAX) {
            throw new IllegalArgumentException("Port out of range: " + port + ". Allowed range [0,65535]");
        }
        this.port = port;
        return this;
    }

    /**
     * Set the SSL Context function which will be used to initialize underlying
     * HTTPs listener for secure connections.
     * <p>
     * For example:
     * <pre>{@code
     * builder.sslContextFn(() -> {
     *     SSLContext context = SSLContext.getInstance("TLS");
     *     KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
     *     KeyStore ks = KeyStore.getInstance("JKS");
     *     char[] password = "123456".toCharArray();
     *     ks.load(new FileInputStream("the.keystore"), password);
     *     kmf.init(ks, password);
     *     TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
     *     KeyStore ts = KeyStore.getInstance("JKS");
     *     ts.load(new FileInputStream("the.truststore"), password);
     *     tmf.init(ts);
     *     context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
     *     return context;
     * })
     * }</pre>
     * <p>
     * Default value is {@code null}. By setting this parameter you create a
     * HTTPs listener source instead of a HTTP listener source.
     *
     * @param sslContextFn the function to create {@link SSLContext} which used
     *                     to initialize underlying HTTPs listener for secure
     *                     connections.
     */
    @Nonnull
    public HttpListenerSourceBuilder<T> sslContextFn(@Nonnull SupplierEx<SSLContext> sslContextFn) {
        this.sslContextFn = Objects.requireNonNull(sslContextFn);
        return this;
    }

    /**
     * Set the function which converts the received payload to the pipeline
     * item.
     * <p>
     * For example to convert the payload to a {@link String}:
     * <pre>{@code
     * builder.mapToItemFn(data -> new String(data))
     * }</pre>
     * <p>
     * By default payload is converted to String. If this parameter is set
     * {@link #type(Class)} is ignored.
     *
     * @param mapToItemFn the function which converts the received payload to
     *                    pipeline item.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> HttpListenerSourceBuilder<T_NEW> mapToItemFn(@Nonnull FunctionEx<byte[], T_NEW> mapToItemFn) {
        HttpListenerSourceBuilder<T_NEW> newThis = (HttpListenerSourceBuilder<T_NEW>) this;
        newThis.mapToItemFn = Objects.requireNonNull(mapToItemFn);
        return newThis;
    }

    /**
     * Set the type of the object which the source will map the JSON formatted
     * payload. The payload is converted to {@link String} first and then
     * mapped to the given type.
     * <p>
     * For example to convert the payload to a person object:
     * <pre>{@code
     * builder.type(Person.class)
     * }</pre>
     * <p>
     * Default value is {@code null}. If {@link #mapToItemFn(FunctionEx)} is
     * set this parameter is ignored.
     *
     * @param type the type of the object which the source will map the JSON
     *             formatted payload.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> HttpListenerSourceBuilder<T_NEW> type(@Nonnull Class<T_NEW> type) {
        HttpListenerSourceBuilder<T_NEW> newThis = (HttpListenerSourceBuilder<T_NEW>) this;
        newThis.type = Objects.requireNonNull(type);
        return newThis;
    }

    /**
     * Build a HTTP(s) Listener {@link StreamSource} with the supplied
     * parameters.
     */
    @Nonnull
    public StreamSource<T> build() {
        int localPort = port;
        FunctionEx<byte[], T> mapFn = mapFn();
        SupplierEx<SSLContext> sslFn = sslContextFn;

        return SourceBuilder.stream(name(),
                ctx -> new HttpListenerSourceContext<>(ctx, localPort, sslFn, mapFn))
                .<T>fillBufferFn(HttpListenerSourceContext::fillBuffer)
                .destroyFn(HttpListenerSourceContext::close)
                .distributed(1)
                .build();
    }

    private String name() {
        if (sslContextFn == null) {
            return "http-listener[" + port + "]";
        }
        return "https-listener[" + port + "]";
    }

    @SuppressWarnings("unchecked")
    private FunctionEx<byte[], T> mapFn() {
        if (mapToItemFn != null) {
            return mapToItemFn;
        }
        if (type != null) {
            return data -> JsonUtil.beanFrom(new String(data), type);
        }
        return data -> (T) new String(data, StandardCharsets.UTF_8);
    }
}
