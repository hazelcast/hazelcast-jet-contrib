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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
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
import java.io.IOException;
import java.io.Serializable;
import java.security.KeyStore;
import java.util.Objects;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class HttpListenerSourceTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testHttpIngestion_with_objectMapping() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int portOffset = 100;
        String httpEndpoint1 = getEndpointAddress(jet, portOffset, false);
        String httpEndpoint2 = getEndpointAddress(jet2, portOffset, false);

        Pipeline p = Pipeline.create();

        p.readFrom(HttpListenerSources.httpListener(portOffset, User.class))
                .withoutTimestamps()
                .filter(user -> user.id >= 80)
                .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint1, httpEndpoint2);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpIngestion() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        int portOffset = 100;
        String httpEndpoint1 = getEndpointAddress(jet, portOffset, false);
        String httpEndpoint2 = getEndpointAddress(jet2, portOffset, false);

        Pipeline p = Pipeline.create();

        p.readFrom(HttpListenerSources.httpListener(portOffset))
                .withoutTimestamps()
                .filter(map -> (int) map.get("id") >= 80)
                .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint1, httpEndpoint2);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpsIngestion_with_objectMapping() throws Throwable {
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
        JetInstance jet2 = createJetMember();

        int portOffset = 100;
        String httpEndpoint1 = getEndpointAddress(jet, portOffset, true);
        String httpEndpoint2 = getEndpointAddress(jet2, portOffset, true);

        Pipeline p = Pipeline.create();

        p.readFrom(HttpListenerSources.httpsListener(portOffset, contextSupplier, User.class))
                .withoutTimestamps()
                .filter(user -> user.id >= 80)
                .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLContext(contextSupplier.get())
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                .build();
        postUsers(httpClient, 100, httpEndpoint1, httpEndpoint2);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    @Test
    public void testHttpsIngestion() throws Throwable {
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
        JetInstance jet2 = createJetMember();

        int portOffset = 100;
        String httpEndpoint1 = getEndpointAddress(jet, portOffset, true);
        String httpEndpoint2 = getEndpointAddress(jet2, portOffset, true);

        Pipeline p = Pipeline.create();

        p.readFrom(HttpListenerSources.httpsListener(portOffset, contextSupplier))
                .withoutTimestamps()
                .filter(map -> (int) map.get("id") >= 80)
                .writeTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLContext(contextSupplier.get())
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                .build();
        postUsers(httpClient, 100, httpEndpoint1, httpEndpoint2);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    private String getEndpointAddress(JetInstance jet, int portOffset, boolean ssl) {
        Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
        int port = localAddress.getPort() + portOffset;
        String hostPort = localAddress.getHost() + ":" + port;
        return ssl ? "https://" + hostPort : "http://" + hostPort;
    }

    private void postUsers(CloseableHttpClient httpClient, int count, String uri1, String uri2) throws IOException {
        for (int i = 0; i < count; i++) {
            User user = new User(i, "name" + i);
            String jsonString = JsonUtil.toJson(user);
            StringEntity requestEntity = new StringEntity(
                    jsonString,
                    ContentType.APPLICATION_JSON);
            HttpPost post = i % 2 == 0 ? new HttpPost(uri1) : new HttpPost(uri2);
            post.setEntity(requestEntity);
            CloseableHttpResponse response = httpClient.execute(post);
            response.getEntity().getContent().close();
        }
    }

    public static class User implements Serializable {
        private int id;
        private String name;

        public User() {
        }

        public User(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            User user = (User) o;
            return Objects.equals(id, user.id) &&
                    Objects.equals(name, user.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public String toString() {
            return "User{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
