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

package com.hazelcast.jet.contrib.redis;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RedisSinkTest extends JetTestSupport {

    @Rule
    public RedisContainer container = new RedisContainer();
    private RedisClient client;
    private RedisURI uri;
    private StatefulRedisConnection<String, String> connection;

    private JetInstance instance;

    @Before
    public void setup() {
        client = container.newRedisClient();
        connection = client.connect();
        uri = RedisURI.create(container.connectionString());

        instance = createJetMember();
    }

    @After
    public void teardown() {
        terminateInstance(instance);

        connection.close();
        client.shutdown();
    }

    @Test
    public void hash() {
        long itemCount = 100_000;

        IMap<String, String> map = instance.getMap("map");
        for (int i = 0; i < itemCount; i++) {
            map.put("foo-" + i, "bar-" + i);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(map))
                .writeTo(RedisSinks.hash("sink", uri, "hash"));

        instance.newJob(p).join();

        RedisCommands<String, String> sync = connection.sync();
        assertEquals(itemCount, sync.hkeys("hash").size());
        assertEquals("bar-999", sync.hget("hash", "foo-999"));
    }

    @Test
    public void sortedSet() {
        long itemCount = 100_000;

        List<Integer> shuffledList = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            shuffledList.add(i);
        }
        Collections.shuffle(shuffledList);

        IList<Integer> list = instance.getList("list");
        list.addAll(shuffledList);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer>list("list"))
                .map(i -> ScoredValue.fromNullable(i, "foobar-" + i))
                .writeTo(RedisSinks.sortedSet("sink", uri, "sortedSet"));

        instance.newJob(p).join();

        RedisCommands<String, String> sync = connection.sync();
        long rangeSize = sync.zcount("sortedSet",
                Range.from(
                        Range.Boundary.including(10_000),
                        Range.Boundary.excluding(90_000)
                )
        );
        assertEquals(80_000L, rangeSize);
    }

    @Test
    public void stream() {
        IList<String> list = instance.getList("list");
        for (int i = 0; i < 10; i++) {
            list.add("key-" + i);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
                .writeTo(RedisSinks.stream("source", uri, "stream"));

        instance.newJob(p).join();

        RedisCommands<String, String> sync = connection.sync();
        List<StreamMessage<String, String>> messages = sync.xread(XReadArgs.StreamOffset.from("stream", "0"));
        assertEquals(list.size(), messages.size());
    }

}
