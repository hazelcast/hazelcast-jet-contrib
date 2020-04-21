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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.slack.util.SlackResponse;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
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
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains the methods to create Slack sinks.
 */
public final class SlackSinks {

    private static final String URL = "https://slack.com/api/chat.postMessage";

    private static final int RETRY_COUNT = Integer.MAX_VALUE;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SlackSinks() {
    }

    /**
     * Creates a slack sink to send messages to the requested channelId.
     * Internally Apache {@link org.apache.http.client.HttpClient} is used with
     * custom {@link HttpRequestRetryHandler} to post the messages.
     * Following exceptions {@link ConnectTimeoutException}, {@link HttpHostConnectException},
     * {@link IOException}, {@link UnknownHostException}
     * are handled through custom retry handler.
     * Other error scenarios will lead to the failure of the Jet job.
     *
     * @param accessToken String Bearer token to authenticate the slack web api requests
     * @param slackChannelId String Unique channel id to send messages to the slack channel.
     * @return
     */
    public static Sink sink(String accessToken, String slackChannelId) {
        return SinkBuilder.sinkBuilder("slack(" + slackChannelId + ")",
                ctx -> new SlackContext(() -> HttpClients.custom().setRetryHandler(httpRetryHandler())
                        .build(), accessToken, slackChannelId))
                .<String>receiveFn((ctx, item) -> ctx.receiveFn(item)).destroyFn(ctx -> ctx.destroy()).build();
    }

    private static HttpRequestRetryHandler httpRetryHandler() {
        return (exception, executionCount, context) -> {
            if (executionCount >= RETRY_COUNT) {
                // Do not retry if over max retry count
                return false;
            }
            if (exception instanceof UnknownHostException) {
                // Unknown host
                return true;
            }
            if (exception instanceof ConnectTimeoutException) {
                // Connection refused
                return true;
            }
            if (exception instanceof HttpHostConnectException) {
                // connection exception
                return true;
            }
            if (exception instanceof IOException) {
                // Interrupted Io exceptions
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
        };
    }

    private static final class SlackContext {

        private final SupplierEx<CloseableHttpClient> httpClientSupplierEx;
        private final CloseableHttpClient closeableHttpClient;
        private final String accessToken;
        private final String channel;

        private SlackContext(SupplierEx<CloseableHttpClient> bulkRequestSupplier, String accessToken, String channel) {
            this.httpClientSupplierEx = bulkRequestSupplier;
            this.closeableHttpClient = bulkRequestSupplier.get();
            this.accessToken = accessToken;
            this.channel = channel;
        }

        private String receiveFn(String message) {
            HttpPost request =  new HttpPost(URL);
            CloseableHttpResponse response = null;
            String result = "";
            SlackResponse slackResponse;
            try {
                // add request headers
                request.addHeader("Authorization", String.format("Bearer %s", accessToken));
                List<NameValuePair> urlParameters = new ArrayList<>();
                urlParameters.add(new BasicNameValuePair("channel",  channel));
                urlParameters.add(new BasicNameValuePair("text", message));
                request.setEntity(new UrlEncodedFormEntity(urlParameters));
                response = (CloseableHttpResponse) closeableHttpClient.execute(request);
                result = EntityUtils.toString(response.getEntity());
                slackResponse = MAPPER.readValue(result, SlackResponse.class);
                if (!slackResponse.isOk()) {
                    throw new RuntimeException(slackResponse.getError());
                }
            } catch (IOException var) {
                throw ExceptionUtil.rethrow(var);
            } finally {
                try {
                    request.releaseConnection();
                    response.close();
                } catch (Exception var) {
                    throw ExceptionUtil.rethrow(var);
                }
            }
            return result;
        }

        private void destroy() throws IOException {
            closeableHttpClient.close();
        }
    }
}
