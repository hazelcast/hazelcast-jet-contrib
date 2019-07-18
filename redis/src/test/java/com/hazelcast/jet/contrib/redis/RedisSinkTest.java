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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class RedisSinkTest extends JetTestSupport {

    @Rule
    public RedisContainer container = new RedisContainer();
    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;

    private JetInstance instance;

    @Before
    public void setup() {
        client = container.newRedisClient();
        connection = client.connect();

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

        IMapJet<String, String> map = instance.getMap("map");
        for (int i = 0; i < itemCount; i++) {
            map.put("foo-" + i, "bar-" + i);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map))
                .drainTo(RedisSinks.hash("sink", container.connectionString(), "hash"));

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

        IListJet<Integer> list = instance.getList("list");
        list.addAll(shuffledList);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer>list("list"))
                .map(i -> ScoredValue.fromNullable(i, "foobar-" + i))
                .drainTo(RedisSinks.sortedSet("sink", container.connectionString(), "sortedSet"));

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
        IListJet<String> list = instance.getList("list");
        for (int i = 0; i < 10; i++) {
            list.add("key-" + i);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list(list))
                .drainTo(RedisSinks.stream("source", container.connectionString(), "stream"));

        instance.newJob(p).join();

        RedisCommands<String, String> sync = connection.sync();
        List<StreamMessage<String, String>> messages = sync.xread(XReadArgs.StreamOffset.from("stream", "0"));
        assertEquals(list.size(), messages.size());
    }


    @Test
    @Ignore
    public void benchmark() {
        RedisAsyncCommands<String, String> async = connection.async();

        long itemCount = 100_000;
        int streamCount = 1;

        for (int s = 0; s < streamCount; s++) {
            for (int i = 0; i < itemCount; i++) {
                async.xadd("stream-" + s, "key-" + i, "val-" + i);
            }
        }

        Map<String, String> streamOffsets = new HashMap<>();
        for (int i = 0; i < streamCount; i++) {
            streamOffsets.put("stream-" + i, "0");
        }

        Pipeline p = Pipeline.create();

        String redisURI = container.connectionString();
        p.drawFrom(RedisSources.stream("source", redisURI, streamOffsets, StreamMessage::getBody))
                .withoutTimestamps()
                .drainTo(RedisSinks.stream("sink", RedisURI.create(redisURI), "sinkStream"));


        long begin = System.currentTimeMillis();

        instance.newJob(p);

        assertTrueEventually(() -> {
            long size = async.xlen("sinkStream").get(1, MINUTES);
            assertEquals(itemCount * streamCount, size);
        });
        long elapsed = System.currentTimeMillis() - begin;
        System.out.println("qwe " + elapsed);

    }

    @Test
    @Ignore
    public void benchmark2() {
        RedisAsyncCommands<String, String> async = connection.async();

        long itemCount = 1_000_000;
        for (int i = 0; i < itemCount; i++) {
            async.xadd("stream", "key-" + i, "val-" + i);
        }

        long begin = System.currentTimeMillis();

        RedisCommands<String, String> sync = connection.sync();
        XReadArgs readArgs = XReadArgs.Builder.block(100).count(1000);

        int count = 0;
        String offset = "0";

        while (count < itemCount) {

            List<StreamMessage<String, String>> messages =
                    sync.xread(readArgs, XReadArgs.StreamOffset.from("stream", offset));
            int size = messages.size();
            count += size;
            offset = messages.get(size - 1).getId();
        }

        long elapsed = System.currentTimeMillis() - begin;
        System.out.println(elapsed);
    }

}
