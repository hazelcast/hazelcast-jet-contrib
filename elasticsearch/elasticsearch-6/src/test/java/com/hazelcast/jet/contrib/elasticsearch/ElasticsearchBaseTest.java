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

package com.hazelcast.jet.contrib.elasticsearch;

import com.hazelcast.collection.IList;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public abstract class ElasticsearchBaseTest extends JetTestSupport {

    private static final int OBJECT_COUNT = 20;

    @Rule
    public ElasticsearchContainer container =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:6.8.0")
            .withNetwork(Network.newNetwork());

    JetInstance jet;
    IList<User> userList;
    String indexName = "users";
    private RestHighLevelClient client;

    @Before
    public void setup() {
        container.start();
        client = createClient(container.getHttpHostAddress());

        jet = createJetMember();

        userList = jet.getList("userList");
        for (int i = 0; i < OBJECT_COUNT; i++) {
            userList.add(new User("user-" + i, i));
        }
    }

    @After
    public void cleanup() throws IOException {
        container.stop();
        client.close();
    }

    void assertIndexes() throws IOException {
        for (int i = 0; i < OBJECT_COUNT; i++) {
            GetRequest request = new GetRequest(indexName).id(String.valueOf(i));
            assertTrue(client.exists(request));
        }
    }

    static RestHighLevelClient createClient(String containerAddress) {
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(containerAddress)));
    }

    static FunctionEx<User, IndexRequest> indexFn(String indexName) {
        return user -> {
            IndexRequest request = new IndexRequest(indexName, "doc", String.valueOf(user.age));
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("name", user.name);
            jsonMap.put("age", user.age);
            request.source(jsonMap);
            return request;
        };
    }

    static final class User implements Serializable {

        final String name;
        final int age;

        User(String name, int age) {
            this.name = name;
            this.age = age;
        }

    }
}
