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
package com.hazelcast.jet.contrib.slack;

import com.hazelcast.function.Functions;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SlackSInksTest extends JetTestSupport {

    private JetInstance jet;
    private Properties credentials;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @Test
    public void testSlackSink() {
        List<String> text = jet.getList("text");
        text.add("hello world hello world hazelcast");
        text.add("sample message to slack channel");

        Pipeline pipeline = Pipeline.create();
        BatchStage<Map.Entry<String, Long>> tweets = pipeline
                .readFrom(Sources.<String>list("text"))
                .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty())
                .groupingKey(Functions.wholeItem())
                .aggregate(AggregateOperations.counting());

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list("text"))
                .writeTo(SlackSinks.sink(System.getenv("ACCESS_TOKEN"), System.getenv("CHANNEL_ID")));
        jet.newJob(p).join();
    }

    @Test
    public void retryTests() {

        HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(
                    IOException exception,
                    int executionCount,
                    HttpContext context) {
                if (executionCount >= 5) {
                    // Do not retry if over max retry count
                    return false;
                }
                if (exception instanceof InterruptedIOException) {
                    // Timeout
                    return false;
                }
                if (exception instanceof UnknownHostException) {
                    // Unknown host
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) {
                    // Connection refused
                    return true;
                }
                if (exception instanceof HttpHostConnectException) {
                    // connection exception
                    return true;
                }
                HttpClientContext clientContext = HttpClientContext.adapt(context);
                HttpRequest request = clientContext.getRequest();
                boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
                if (idempotent) {
                    // Retry if the request is considered idempotent
                    return true;
                }
                return false;
            }

        };

        CloseableHttpClient closeableHttpClient = HttpClients.custom()
                .setRetryHandler(myRetryHandler)
                .build();
        HttpPost request =  new HttpPost("http://localhost:8080/hello");
        CloseableHttpResponse response = null;
        String result = "";
        try {
            // add request headers
            request.addHeader("Authorization", String.format("Bearer %s",System.getenv("ACCESS_TOKEN")));
            List<NameValuePair> urlParameters = new ArrayList<>();
            urlParameters.add(new BasicNameValuePair("channel",  "random"));
            urlParameters.add(new BasicNameValuePair("text", "message"));
            request.setEntity(new UrlEncodedFormEntity(urlParameters));
            response = (CloseableHttpResponse) closeableHttpClient.execute(request);
            result = EntityUtils.toString(response.getEntity());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                request.releaseConnection();
                response.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
