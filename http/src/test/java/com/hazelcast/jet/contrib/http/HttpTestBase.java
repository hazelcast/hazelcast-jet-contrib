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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.http.domain.User;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;

public class HttpTestBase extends JetTestSupport {

    private static final SupplierEx<TrustManagerFactory> TRUST_MANAGER_FACTORY_FN = () -> {
        char[] password = "123456".toCharArray();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = KeyStore.getInstance("JKS");
        File tsfile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.trustStore);
        ts.load(new FileInputStream(tsfile), password);
        tmf.init(ts);
        return tmf;
    };

    private static final SupplierEx<SSLContext> SSL_CONTEXT_FN = () -> {
        SSLContext context = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] password = "123456".toCharArray();
        File tempFile = TestKeyStoreUtil.createTempFile(TestKeyStoreUtil.keyStore);
        ks.load(new FileInputStream(tempFile), password);
        kmf.init(ks, password);
        TrustManagerFactory trustManagerFactory = TRUST_MANAGER_FACTORY_FN.get();

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        context.init(kmf.getKeyManagers(), trustManagers, null);
        return context;
    };

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    HazelcastInstance hz;
    CloseableHttpClient httpClient;
    CloseableHttpClient httpsClient;

    public static SupplierEx<SSLContext> sslContextFn() {
        return SSL_CONTEXT_FN;
    }

    public static X509TrustManager x509TrustManager() {
        TrustManager[] trustManagers = TRUST_MANAGER_FACTORY_FN.get().getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        throw new IllegalStateException("No X509TrustManager found");
    }

    @Before
    public void setup() {
        hz = createHazelcastInstance(config());
        httpClient = HttpClients.createDefault();
        httpsClient = HttpClients
                .custom()
                .setSSLContext(sslContextFn().get())
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setRetryHandler(new DefaultHttpRequestRetryHandler(10, true))
                .build();
    }

    @After
    public void cleanup() throws IOException {
        httpClient.close();
        httpsClient.close();
    }

    public String httpEndpointAddress(int port, boolean ssl) {
        return ssl ? "https://localhost:" + port : "http://localhost:" + port;
    }

    public void postUsers(CloseableHttpClient httpClient, int count, int port, boolean ssl) throws IOException {
        String address = httpEndpointAddress(port, ssl);
        for (int i = 0; i < count; i++) {
            User user = new User(i, "name" + i);
            String jsonString = JsonUtil.toJson(user);
            StringEntity requestEntity = new StringEntity(
                    jsonString,
                    ContentType.APPLICATION_JSON);
            HttpPost post = new HttpPost(address);
            post.setEntity(requestEntity);
            CloseableHttpResponse response = executeWithRetry(httpClient, post);
            response.getEntity().getContent().close();
        }
    }

    private CloseableHttpResponse executeWithRetry(CloseableHttpClient httpClient, HttpPost post) {
        for (int i = 0; i < 30; i++) {
            try {
                return httpClient.execute(post);
            } catch (Exception e) {
                logger.warning(e.getMessage());
                sleepAtLeastMillis(100);
            }
        }
        throw new AssertionError("Failed to execute the post");
    }

    static Config config() {
        Config cfg = new Config();
        cfg.getMetricsConfig().setCollectionFrequencySeconds(1);
        return cfg;
    }
}
