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

import com.hazelcast.collection.IQueue;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_COUNT;
import static com.launchdarkly.eventsource.ReadyState.OPEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HttpListenerSinkTest extends HttpTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private XnioWorker worker;
    private ByteBufferPool buffer;
    private Job job;
    private Closeable wsOrSseClient;

    @Before
    public void setUp() throws Exception {
        createJetMember(jetConfig());
        Xnio xnio = Xnio.getInstance(HttpListenerSinkTest.class.getClassLoader());
        worker = xnio.createWorker(OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, 2)
                .getMap());

        buffer = new DefaultByteBufferPool(true, 256);
    }

    @After
    public void after() throws IOException {
        worker.shutdown();
        buffer.close();

        // cleanup
        if (job != null) {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
        if (wsOrSseClient != null) {
            wsOrSseClient.close();
        }
    }

    @Test
    public void testWebsocket_when_clientConnectsAfterAccumulation() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .accumulateItems(100)
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_accumulateEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .accumulateItems(100)
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount * 2, queue);
    }

    @Test
    public void testWebsocket_when_accumulateEnabledWithSmallNumber() {
        // Given
        int queueLimit = 5;
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .accumulateItems(queueLimit)
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount + queueLimit, queue);
    }

    @Test
    public void testWebsocket_when_accumulateDisabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;

        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_sslEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .sslContextFn(sslContextFn())
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_mutualAuthEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .sslContextFn(sslContextFn())
                .enableMutualAuthentication()
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_portConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .port(8091)
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        assertTrue(webSocketAddress.endsWith("8091/"));
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_pathConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .path("/user")
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        assertTrue(webSocketAddress.endsWith("/user"));
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testWebsocket_when_toStringFnConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .toStringFn(item -> item.toString().toUpperCase())
                .buildWebsocket();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String webSocketAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromWebSocket(webSocketAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
        assertEquals(messageCount, queue.stream().filter(s -> s.startsWith("MESSAGE-")).count());
    }

    @Test
    public void testSSE_when_clientConnectsAfterAccumulation() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .accumulateItems(100)
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;

        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_accumulateEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .accumulateItems(100)
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;

        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount * 2, queue);
    }

    @Test
    public void testSSE_when_accumulateDisabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;

        postMessages(sourceQueue, messageCount);
        assertReceivedCountEventually(messageCount);

        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_sslEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .sslContextFn(sslContextFn())
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_mutualAuthEnabled() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .sslContextFn(sslContextFn())
                .enableMutualAuthentication()
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_portConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .port(8091)
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        assertTrue(sseAddress.endsWith("8091/"));
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_pathConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .path("/user")
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        assertTrue(sseAddress.endsWith("/user"));
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
    }

    @Test
    public void testSSE_when_toStringFnConfigured() {
        // Given
        IQueue<String> sourceQueue = jet.getHazelcastInstance().getQueue(randomName());
        Sink<Object> sink = HttpListenerSinks.builder()
                .toStringFn(item -> item.toString().toUpperCase())
                .buildServerSent();
        startJob(sourceQueue, sink);

        // when
        int messageCount = 10;
        String sseAddress = HttpListenerSinks.sinkAddress(jet, job);
        Collection<String> queue = new ArrayBlockingQueue<>(messageCount * 2);
        receiveFromSse(sseAddress, queue);
        postMessages(sourceQueue, messageCount);

        // test
        assertSizeEventually(messageCount, queue);
        assertEquals(messageCount, queue.stream().filter(s -> s.startsWith("MESSAGE-")).count());
    }

    private void receiveFromSse(String sseAddress, Collection<String> queue) {
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
            clientBuilder.sslSocketFactory(sslContextFn().get().getSocketFactory(), x509TrustManager());
            clientBuilder.hostnameVerifier(new NoopHostnameVerifier());
        }
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(sseAddress));
        builder.client(clientBuilder.build());
        EventSource eventSource = builder.build();
        eventSource.start();
        assertTrueEventually(() -> assertSame(OPEN, eventSource.getState()));
        wsOrSseClient = eventSource;
    }

    private void receiveFromWebSocket(String webSocketAddress, Collection<String> queue) {
        WebSocketChannel wsChannel = connectWithRetry(webSocketAddress);
        wsChannel.getReceiveSetter().set(new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                queue.add(message.getData());
            }
        });
        wsChannel.resumeReceives();
        wsOrSseClient = wsChannel;
    }

    private WebSocketChannel connectWithRetry(String webSocketAddress) {
        Xnio xnio = Xnio.getInstance(HttpListenerSinkTest.class.getClassLoader());
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

    private void startJob(IQueue<String> sourceQueue, Sink<Object> sink) {
        Pipeline p = Pipeline.create();
        p.readFrom(queueSource(sourceQueue))
                .withoutTimestamps()
                .writeTo(sink);
        JobConfig jobConfig = new JobConfig().setName(sourceQueue.getName());
        job = jet.newJob(p, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);
    }

    StreamSource<String> queueSource(IQueue<String> sourceQueue) {
        String name = sourceQueue.getName();
        return SourceBuilder.stream(name, c -> new QueueSourceContext(c, name))
                .fillBufferFn(QueueSourceContext::fillBuffer)
                .build();
    }

    void postMessages(IQueue<String> sourceQueue, int messageCount) {
        String pattern = "message-%d";
        for (int i = 0; i < messageCount; i++) {
            sourceQueue.offer(String.format(pattern, i));
        }
    }

    void assertReceivedCountEventually(int expected) {
        assertTrueEventually(() -> assertEquals(expected,
                job.getMetrics().get(RECEIVED_COUNT).stream().mapToLong(Measurement::value).sum()));
    }

    static class QueueSourceContext {

        static final int DRAIN_LIMIT = 256;

        List<String> tempCollection = new ArrayList<>();
        IQueue<String> queue;

        QueueSourceContext(Processor.Context c, String name) {
            queue = c.jetInstance().getHazelcastInstance().getQueue(name);
        }

        public void fillBuffer(SourceBuilder.SourceBuffer<String> buffer) {
            queue.drainTo(tempCollection, DRAIN_LIMIT);
            tempCollection.forEach(buffer::add);
            tempCollection.clear();
        }
    }
}
