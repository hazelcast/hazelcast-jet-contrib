/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.contrib.http.marshalling.impl.HazelcastInternalJsonMarshallingStrategy;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.nio.Address;
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
import java.security.KeyStore;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class HttpListenerSourceTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testHttpIngestion() throws Throwable {
        JetInstance jet = createJetMember();
        JetInstance jet2 = createJetMember();

        Address localAddress = jet2.getHazelcastInstance().getCluster().getLocalMember().getAddress();
        int portOffset = 100;
        int port = localAddress.getPort() + portOffset;
        String httpEndpoint = "http://" + localAddress.getHost() + ":" + port;

        Pipeline p = Pipeline.create();

        p.drawFrom(HttpListenerSources.httpListener(portOffset, new HazelcastInternalJsonMarshallingStrategy()))
         .withoutTimestamps()
         .filter(object -> object.asObject().get("id").asInt() >= 80)
         .drainTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        postUsers(httpClient, 100, httpEndpoint);

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

        Address localAddress = jet2.getHazelcastInstance().getCluster().getLocalMember().getAddress();
        int portOffset = 100;
        int port = localAddress.getPort() + portOffset;
        String httpsEndpoint = "https://" + localAddress.getHost() + ":" + port;

        Pipeline p = Pipeline.create();

        p.drawFrom(HttpListenerSources.httpsListener(portOffset, contextSupplier, new HazelcastInternalJsonMarshallingStrategy()))
         .withoutTimestamps()
         .map(JsonValue::asObject)
         .filter(object -> object.get("id").asInt() >= 80)
         .drainTo(assertCollectedEventually(30, list -> assertEquals(20, list.size())));

        Job job = jet.newJob(p);

        assertJobStatusEventually(job, JobStatus.RUNNING);
        sleepAtLeastSeconds(3);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                    .setSSLContext(contextSupplier.get())
                                                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                                                    .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                                                    .build();
        postUsers(httpClient, 100, httpsEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    private void postUsers(CloseableHttpClient httpClient, int count, String uri) throws IOException {
        for (int i = 0; i < count; i++) {
            JsonObject object = new JsonObject();
            object.add("id", i);
            object.add("name", "name" + i);
            StringEntity requestEntity = new StringEntity(
                    object.toString(),
                    ContentType.APPLICATION_JSON);
            HttpPost post = new HttpPost(uri);
            post.setEntity(requestEntity);
            CloseableHttpResponse response = httpClient.execute(post);
            response.getEntity().getContent().close();
        }
    }

}