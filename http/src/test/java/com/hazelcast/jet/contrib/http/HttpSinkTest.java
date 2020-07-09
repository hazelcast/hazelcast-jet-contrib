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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import io.undertow.connector.ByteBufferPool;
import io.undertow.protocols.ssl.UndertowXnioSsl;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.websockets.client.WebSocketClient;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import okhttp3.OkHttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;

import static com.hazelcast.jet.contrib.http.HttpSinkBuilder.DEFAULT_PATH;
import static com.hazelcast.jet.contrib.http.HttpSinkBuilder.DEFAULT_PORT;
import static com.launchdarkly.eventsource.ReadyState.OPEN;
import static org.junit.Assert.assertSame;

public class HttpSinkTest extends HttpTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private XnioWorker worker;
    private ByteBufferPool buffer;

    @Before
    public void setUp() throws Exception {
        Xnio xnio = Xnio.getInstance(HttpSinkTest.class.getClassLoader());
        worker = xnio.createWorker(OptionMap.builder()
                                            .set(Options.WORKER_IO_THREADS, 2)
                                            .getMap());

        this.buffer = new DefaultByteBufferPool(true, 256);
    }

    @After
    public void after() {
        worker.shutdown();
        buffer.close();
    }

    @Test
    public void testWebsocketServer_with_accumulate() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .accumulateItems()
                                     .buildWebsocket();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String webSocketAddress = HttpSinks.webSocketAddress(jet);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        WebSocketChannel wsChannel = receiveFromWebSocket(webSocketAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount * 2, queue));

        // cleanup
        cleanup(job, wsChannel);
    }

    @Test
    public void testWebsocketServer_without_accumulate() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .buildWebsocket();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String webSocketAddress = HttpSinks.webSocketAddress(jet);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        WebSocketChannel wsChannel = receiveFromWebSocket(webSocketAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount, queue));

        // cleanup
        cleanup(job, wsChannel);
    }

    @Test
    public void testWebsocketServer_with_ssl() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .sslContextFn(sslContextFn())
                                     .buildWebsocket();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String webSocketAddress = HttpSinks.webSocketAddress(jet, DEFAULT_PORT, DEFAULT_PATH, true);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        WebSocketChannel wsChannel = receiveFromWebSocket(webSocketAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount, queue));

        // cleanup
        cleanup(job, wsChannel);
    }

    @Test
    public void testSSEServer_with_accumulate() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .accumulateItems()
                                     .buildServerSent();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String sseAddress = HttpSinks.sseAddress(jet);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        EventSource eventSource = receiveFromSse(sseAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount * 2, queue));

        // cleanup
        cleanup(job, eventSource);
    }

    @Test
    public void testSSEServer_without_accumulate() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .buildServerSent();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String sseAddress = HttpSinks.sseAddress(jet);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        EventSource eventSource = receiveFromSse(sseAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount, queue));

        // cleanup
        cleanup(job, eventSource);
    }

    @Test
    public void testSSEServer_with_ssl() throws Throwable {
        // Given
        Sink<Object> sink = HttpSinks.builder()
                                     .sslContextFn(sslContextFn())
                                     .buildServerSent();
        Job job = startJob(sink);

        // when
        int messageCount = 10;

        String httpEndpoint = httpEndpointAddress(jet, HttpListenerBuilder.DEFAULT_PORT, false);
        postUsers(httpClient, messageCount, httpEndpoint);

        String sseAddress = HttpSinks.sseAddress(jet, DEFAULT_PORT, DEFAULT_PATH, true);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        EventSource eventSource = receiveFromSse(sseAddress, queue);
        postUsers(httpClient, messageCount, httpEndpoint);

        // test
        assertTrueEventually(() -> assertSizeEventually(messageCount, queue));

        // cleanup
        cleanup(job, eventSource);
    }

    private EventSource receiveFromSse(String sseAddress, Collection<String> queue) {
        EventHandler eventHandler = new EventHandler() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClosed() {
            }

            @Override
            public void onMessage(String event, MessageEvent messageEvent) {
                queue.add(messageEvent.getData());
            }

            @Override
            public void onComment(String comment) {
            }

            @Override
            public void onError(Throwable t) {
            }
        };
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        if (sseAddress.startsWith("https")) {
            clientBuilder.sslSocketFactory(sslContextFn().get().getSocketFactory());
            clientBuilder.hostnameVerifier(new NoopHostnameVerifier());
        }
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(sseAddress));
        builder.client(clientBuilder.build());
        EventSource eventSource = builder.build();
        eventSource.start();
        assertTrueEventually(() -> assertSame(OPEN, eventSource.getState()));
        return eventSource;
    }

    private WebSocketChannel receiveFromWebSocket(String webSocketAddress, Collection<String> queue) throws Exception {
        WebSocketChannel wsChannel = connectWithRetry(webSocketAddress);
        wsChannel.getReceiveSetter().set(new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                queue.add(message.getData());
            }
        });
        wsChannel.resumeReceives();
        return wsChannel;
    }

    private WebSocketChannel connectWithRetry(String webSocketAddress) throws Exception {
        Xnio xnio = Xnio.getInstance(HttpSinkTest.class.getClassLoader());
        for (int i = 0; i < 30; i++) {
            try {
                WebSocketClient.ConnectionBuilder builder = WebSocketClient
                        .connectionBuilder(worker, buffer, URI.create(webSocketAddress));
                if (webSocketAddress.startsWith("wss")) {
                    builder.setSsl(new UndertowXnioSsl(xnio, OptionMap.EMPTY, sslContextFn().get()));
                }
                return builder.connect().get();
            } catch (Exception e) {
                logger.warning(e.getMessage());
                sleepAtLeastMillis(100);
            }
        }
        throw new AssertionError("Failed to connect to " + webSocketAddress);
    }

    private Job startJob(Sink<Object> sink) {
        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener())
         .withoutTimestamps()
         .writeTo(sink);

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        return job;
    }

    private void cleanup(Job job, Closeable wsOrSseClient) throws IOException {
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED);
        wsOrSseClient.close();
    }
}
