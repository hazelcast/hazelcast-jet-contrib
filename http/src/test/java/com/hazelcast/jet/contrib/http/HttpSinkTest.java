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
import com.hazelcast.jet.contrib.http.domain.User;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import io.undertow.connector.ByteBufferPool;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.websockets.client.WebSocketClient;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.contrib.http.HttpSinks.SinkNoActiveClientPolicy.ACCUMULATE_AND_PUBLISH_WHEN_CONNECTED;
import static com.launchdarkly.eventsource.ReadyState.OPEN;
import static org.junit.Assert.assertEquals;
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
    public void after() throws Exception {
        worker.shutdown();
        buffer.close();
    }

    @Test
    public void testWebsocketServer_with_accumulatePolicy() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int sourcePortOffset = 100;
        int sinkPortOffset = 200;
        String httpEndpoint1 = getHttpEndpointAddress(jet, sourcePortOffset, false);
        String httpEndpoint2 = getHttpEndpointAddress(jet2, sourcePortOffset, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(sourcePortOffset, User.class))
                .withoutTimestamps()
                .writeTo(HttpSinks.websocket("/users", sinkPortOffset, ACCUMULATE_AND_PUBLISH_WHEN_CONNECTED));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        int messageCount = 10;
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);

        String webSocketAddress = HttpSinks.getWebSocketAddress(jet, job);
        ArrayList<String> receivedMessages = new ArrayList<>();
        WebSocketChannel wsChannel = WebSocketClient.connectionBuilder(worker, buffer, URI.create(webSocketAddress))
                .connect().get();
        wsChannel.getReceiveSetter().set(new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) throws IOException {
                receivedMessages.add(message.getData());
            }
        });
        wsChannel.resumeReceives();
        assertTrueEventually(() -> assertSizeEventually(messageCount, receivedMessages));
        httpClient.close();
    }

    @Test
    public void testWebsocketServer_with_dropPolicy() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int sourcePortOffset = 100;
        int sinkPortOffset = 200;
        String httpEndpoint1 = getHttpEndpointAddress(jet, sourcePortOffset, false);
        String httpEndpoint2 = getHttpEndpointAddress(jet2, sourcePortOffset, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(sourcePortOffset, User.class))
                .withoutTimestamps()
                .writeTo(HttpSinks.websocket("/users", sinkPortOffset));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        // post 10 messages upfront, those should be dropped
        CloseableHttpClient httpClient = HttpClients.createDefault();
        int messageCount = 10;
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);

        // start websocket client
        String webSocketAddress = HttpSinks.getWebSocketAddress(jet, job);
        ArrayList<String> receivedMessages = new ArrayList<>();
        WebSocketChannel wsChannel = WebSocketClient.connectionBuilder(worker, buffer, URI.create(webSocketAddress))
                .connect().get();
        wsChannel.getReceiveSetter().set(new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) throws IOException {
                receivedMessages.add(message.getData());
            }
        });
        wsChannel.resumeReceives();

        // post 10 more messages those should be received by the client
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);

        assertTrueEventually(() -> assertSizeEventually(messageCount, receivedMessages));
        httpClient.close();
    }


    @Test
    public void testSSEServer_with_accumulatePolicy() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int sourcePortOffset = 100;
        int sinkPortOffset = 200;
        String httpEndpoint1 = getHttpEndpointAddress(jet, sourcePortOffset, false);
        String httpEndpoint2 = getHttpEndpointAddress(jet2, sourcePortOffset, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(sourcePortOffset, User.class))
                .withoutTimestamps()
                .writeTo(HttpSinks.sse("/users", sinkPortOffset, ACCUMULATE_AND_PUBLISH_WHEN_CONNECTED));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        int messageCount = 10;
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);

        // start sse client
        CountDownLatch latch = new CountDownLatch(10);
        ArrayList<String> messages = new ArrayList<>();
        EventHandler eventHandler = new EventHandler() {
            @Override
            public void onOpen() throws Exception {

            }

            @Override
            public void onClosed() throws Exception {

            }

            @Override
            public void onMessage(String event, MessageEvent messageEvent) throws Exception {
                messages.add(messageEvent.getData());
                latch.countDown();
            }

            @Override
            public void onComment(String comment) throws Exception {

            }

            @Override
            public void onError(Throwable t) {

            }
        };
        String sseAddress = HttpSinks.getSseAddress(jet, job);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(sseAddress));
        EventSource eventSource = builder.build();
        eventSource.start();
        assertTrueEventually(() -> assertSame(OPEN, eventSource.getState()));

        latch.await();
        assertEquals(messageCount, messages.size());
        eventSource.close();
        httpClient.close();
    }

    @Test
    public void testSSEServer_with_dropPolicy() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int sourcePortOffset = 100;
        int sinkPortOffset = 200;
        String httpEndpoint1 = getHttpEndpointAddress(jet, sourcePortOffset, false);
        String httpEndpoint2 = getHttpEndpointAddress(jet2, sourcePortOffset, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(sourcePortOffset, User.class))
                .withoutTimestamps()
                .writeTo(HttpSinks.sse("/users", sinkPortOffset));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        // post 10 messages upfront, those should be dropped
        CloseableHttpClient httpClient = HttpClients.createDefault();
        int messageCount = 10;
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);


        // start sse client
        CountDownLatch latch = new CountDownLatch(10);
        ArrayList<String> messages = new ArrayList<>();
        EventHandler eventHandler = new EventHandler() {
            @Override
            public void onOpen() throws Exception {
            }

            @Override
            public void onClosed() throws Exception {
            }

            @Override
            public void onMessage(String event, MessageEvent messageEvent) throws Exception {
                messages.add(messageEvent.getData());
                latch.countDown();
            }

            @Override
            public void onComment(String comment) throws Exception {
            }

            @Override
            public void onError(Throwable t) {

            }
        };
        String sseAddress = HttpSinks.getSseAddress(jet, job);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(sseAddress));
        EventSource eventSource = builder.build();
        eventSource.start();
        assertTrueEventually(() -> assertSame(OPEN, eventSource.getState()));

        // post 10 more messages those should be received by the client
        postUsers(httpClient, messageCount, httpEndpoint1, httpEndpoint2);
        latch.await();

        assertEquals(messageCount, messages.size());
        eventSource.close();
        httpClient.close();
    }

}
