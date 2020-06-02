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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.contrib.http.domain.User;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

public class HttpTestBase extends JetTestSupport {

    public static String getHttpEndpointAddress(JetInstance jet, int portOffset, boolean ssl) {
        Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
        int port = localAddress.getPort() + portOffset;
        String hostPort = localAddress.getHost() + ":" + port;
        return ssl ? "https://" + hostPort : "http://" + hostPort;
    }

    public static String getWsEndpointAddress(JetInstance jet, int portOffset) {
        Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
        int port = localAddress.getPort() + portOffset;
        String hostPort = localAddress.getHost() + ":" + port;
        return "ws://" + hostPort;
    }

    public static void postUsers(CloseableHttpClient httpClient, int count, String uri1, String uri2) throws IOException {
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
}
