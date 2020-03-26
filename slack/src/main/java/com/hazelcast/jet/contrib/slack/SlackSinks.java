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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains the methods to create Slack sinks.
 */
public final class SlackSinks {

    private static final String URL = "https://slack.com/api/chat.postMessage";

    private SlackSinks() {
    }

    /**
     * Creates a sink to send messages to the slack channel>
     *
     * @param accessToken String Bearer token to authenticate the slack web api requests
     * @param channel String Unique channel id to send messages to the slack channel.
     * @return
     */
    public static Sink sink(String accessToken, String channel) {
        return SinkBuilder.sinkBuilder("slack-sink",
                ctx -> new SlackContext(() -> HttpClients.createDefault(), accessToken, channel))
                .<String>receiveFn((ctx, item) -> ctx.receiveFn(item)).destroyFn(ctx -> ctx.destroy()).build();
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

        private String receiveFn(String message) throws IOException {
            HttpPost request = new HttpPost(URL);
            // add request headers
            request.addHeader("Authorization", String.format("Bearer %s", accessToken));
            List<NameValuePair> urlParameters = new ArrayList<>();
            urlParameters.add(new BasicNameValuePair("channel",  channel));
            urlParameters.add(new BasicNameValuePair("text", message));
            request.setEntity(new UrlEncodedFormEntity(urlParameters));
            CloseableHttpResponse response = (CloseableHttpResponse) closeableHttpClient.execute(request);
            String result = EntityUtils.toString(response.getEntity());
            request.releaseConnection();
            return result;
        }

        private void destroy() throws IOException {
            closeableHttpClient.close();
        }
    }
}
