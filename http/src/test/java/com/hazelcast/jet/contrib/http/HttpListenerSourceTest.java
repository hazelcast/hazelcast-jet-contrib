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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.contrib.http.domain.User;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class HttpListenerSourceTest extends HttpTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testHttpIngestion_with_objectMapping() throws Throwable {
        JetInstance jet = createJetMember();

        int port = 5901;
        String httpEndpoint = getHttpEndpointAddress(jet, port, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(port, User.class))
         .withoutTimestamps()
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpIngestion_with_rawJsonString() throws Throwable {
        JetInstance jet = createJetMember();

        int port = HttpListenerBuilder.DEFAULT_PORT;
        String httpEndpoint1 = getHttpEndpointAddress(jet, port, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener())
         .withoutTimestamps()
         .map(json -> JsonUtil.beanFrom(json, User.class))
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint1);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpIngestion_with_customDeserializer() throws Throwable {
        JetInstance jet = createJetMember();

        int port = 5901;
        String httpEndpoint1 = getHttpEndpointAddress(jet, port, false);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.httpListener(port,
                bytes -> JsonUtil.beanFrom(new String(bytes), User.class)))
         .withoutTimestamps()
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint1);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    @Test
    public void testHttpsIngestion_with_objectMapping() throws Throwable {


        JetInstance jet = createJetMember();

        int port = HttpListenerBuilder.DEFAULT_PORT;
        String httpEndpoint1 = getHttpEndpointAddress(jet, port, true);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.builder().sslContextFn(sslContextFn()).type(User.class).build())
         .withoutTimestamps()
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                    .setSSLContext(sslContextFn().get())
                                                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                                                    .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                                                    .build();
        postUsers(httpClient, 100, httpEndpoint1);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    @Test
    public void testHttpsIngestion_with_rawJsonString() throws Throwable {
        JetInstance jet = createJetMember();

        int port = HttpListenerBuilder.DEFAULT_PORT;
        String httpEndpoint1 = getHttpEndpointAddress(jet, port, true);

        Pipeline p = Pipeline.create();
        p.readFrom(HttpListenerSources.builder().sslContextFn(sslContextFn()).build())
         .withoutTimestamps()
         .map(json -> JsonUtil.beanFrom(json, User.class))
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                    .setSSLContext(sslContextFn().get())
                                                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                                                    .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                                                    .build();
        postUsers(httpClient, 100, httpEndpoint1);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpsIngestion_with_customDeserializer() throws Throwable {
        SupplierEx<SSLContext> contextSupplier = () -> {
            SSLContext context = SSLContext.getInstance("TLS");
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance("JKS");
            char[] password = "123456".toCharArray();
            File tempFile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.keyStore);
            ks.load(new FileInputStream(tempFile), password);
            kmf.init(ks, password);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ts = KeyStore.getInstance("JKS");
            File tsfile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.trustStore);
            ts.load(new FileInputStream(tsfile), password);
            tmf.init(ts);

            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return context;
        };

        JetInstance jet = createJetMember();

        int port = HttpListenerBuilder.DEFAULT_PORT;
        String httpEndpoint1 = getHttpEndpointAddress(jet, port, true);

        StreamSource<User> source = HttpListenerSources
                .builder()
                .sslContextFn(contextSupplier)
                .mapToItemFn(bytes -> JsonUtil.beanFrom(new String(bytes), User.class))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withoutTimestamps()
         .filter(user -> user.getId() >= 80)
         .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                    .setSSLContext(contextSupplier.get())
                                                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                                                    .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                                                    .build();
        postUsers(httpClient, 100, httpEndpoint1);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

}
