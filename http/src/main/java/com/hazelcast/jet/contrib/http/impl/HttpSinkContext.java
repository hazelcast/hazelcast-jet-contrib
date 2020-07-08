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

package com.hazelcast.jet.contrib.http.impl;


import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import io.undertow.server.handlers.sse.ServerSentEventHandler;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Set;

import static io.undertow.Handlers.path;
import static io.undertow.UndertowOptions.ENABLE_HTTP2;

public class HttpSinkContext<T> {

    private final SinkHttpHandler sinkHttpHandler;
    private final Undertow undertow;
    private final ArrayList<String> messageBuffer;
    private final FunctionEx<T, String> toStringFn;

    public HttpSinkContext(
            @Nonnull Processor.Context context,
            @Nonnull String path,
            int port,
            boolean accumulateItems,
            boolean websocket,
            @Nullable SupplierEx<SSLContext> sslContextFn,
            @Nonnull FunctionEx<T, String> toStringFn
    ) {
        this.messageBuffer = accumulateItems ? new ArrayList<>() : null;
        this.sinkHttpHandler = websocket ? new WebSocketSinkHttpHandler() : new ServerSentSinkHttpHandler();
        this.toStringFn = toStringFn;

        Undertow.Builder builder = Undertow.builder();
        if (sslContextFn == null) {
            builder.addHttpListener(port, host(context));
        } else {
            builder.addHttpsListener(port, host(context), sslContextFn.get());
        }

        undertow = builder
                .setServerOption(ENABLE_HTTP2, true)
                .setHandler(path().addPrefixPath(path, sinkHttpHandler.httpHandler()))
                .build();

        undertow.start();
    }

    @SuppressWarnings("unchecked")
    public void receive(Object item) {
        if (sinkHttpHandler.hasConnectedClients()) {
            drainBuffer();
            sinkHttpHandler.send(toStringFn.apply((T)item));
        } else {
            putToBuffer((T)item);
        }
    }

    public void flush() {
        if (sinkHttpHandler.hasConnectedClients()) {
            drainBuffer();
        }
    }

    public void close() {
        undertow.stop();
    }

    private String host(Processor.Context context) {
        return context.jetInstance().getHazelcastInstance().getCluster().getLocalMember().getAddress().getHost();
    }

    private void drainBuffer() {
        if (messageBuffer == null) {
            return;
        }
        messageBuffer.forEach(sinkHttpHandler::send);
        messageBuffer.clear();
    }

    private void putToBuffer(T item) {
        if (messageBuffer == null) {
            return;
        }
        messageBuffer.add(toStringFn.apply(item));
    }

    interface SinkHttpHandler {

        HttpHandler httpHandler();

        void send(String message);

        boolean hasConnectedClients();
    }

    static class WebSocketSinkHttpHandler implements SinkHttpHandler {

        private final WebSocketProtocolHandshakeHandler handler;
        private final Set<WebSocketChannel> peerConnections;

        public WebSocketSinkHttpHandler() {
            handler = Handlers.websocket((exchange, channel) -> {
            });
            peerConnections = handler.getPeerConnections();
        }

        @Override
        public HttpHandler httpHandler() {
            return handler;
        }

        @Override
        public void send(String message) {
            peerConnections.forEach(channel -> WebSockets.sendText(message, channel, null));
        }

        @Override
        public boolean hasConnectedClients() {
            return !peerConnections.isEmpty();
        }
    }

    static class ServerSentSinkHttpHandler implements SinkHttpHandler {

        private final ServerSentEventHandler handler;
        private final Set<ServerSentEventConnection> connections;

        public ServerSentSinkHttpHandler() {
            handler = Handlers.serverSentEvents();
            connections = handler.getConnections();
        }

        @Override
        public HttpHandler httpHandler() {
            return handler;
        }

        @Override
        public void send(String message) {
            connections.forEach(connection -> connection.send(message));
        }

        @Override
        public boolean hasConnectedClients() {
            return !connections.isEmpty();
        }
    }
}
