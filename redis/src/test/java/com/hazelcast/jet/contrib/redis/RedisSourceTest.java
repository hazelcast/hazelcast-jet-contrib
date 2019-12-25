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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;

public class RedisSourceTest extends JetTestSupport {

    @Rule
    public RedisContainer container = new RedisContainer();
    private RedisClient client;
    private RedisURI uri;
    private StatefulRedisConnection<String, String> connection;

    private JetInstance instance;
    private JetInstance instanceToShutDown;

    @Before
    public void setup() {
        client = container.newRedisClient();
        connection = client.connect();
        uri = RedisURI.create(container.connectionString());

        instance = createJetMember();
        instanceToShutDown = createJetMember();
    }

    @After
    public void teardown() {
        terminateInstance(instance);
        terminateInstance(instanceToShutDown);

        connection.close();
        client.shutdown();
    }

    @Test
    public void hash() {
        int elementCount = 1_000_000;
        fillHash("hash", elementCount);

        Pipeline p = Pipeline.create();
        p.readFrom(RedisSources.hash("source", uri, "hash"))
                .writeTo(Sinks.map("map"));

        instance.newJob(p).join();

        IMap<Object, Object> map = instance.getMap("map");
        assertTrueEventually(() -> assertEquals(elementCount, map.size()));
        assertEquals("bar-999", map.get("foo-999"));
    }

    private void fillHash(String hash, int addCount) {
        RedisCommands<String, String> commands = connection.sync();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < addCount; i++) {
            map.put("foo-" + i, "bar-" + i);
            if (map.size() == 10_000) {
                commands.hmset(hash, map);
                map = new HashMap<>();
            }
        }
        if (!map.isEmpty()) {
            commands.hmset(hash, map);
        }
    }

    @Test
    public void sortedSet() {
        int elementCount = 1_000_000;
        fillSortedSet("sortedSet", elementCount);

        int rangeStart = 100_000;
        int rangeEnd = 500_000;

        Pipeline p = Pipeline.create();
        p.readFrom(RedisSources.sortedSet("source", uri, "sortedSet", rangeStart, rangeEnd))
                .map(sv -> (int) sv.getScore() + ":" + sv.getValue())
                .writeTo(Sinks.list("list"));

        instance.newJob(p).join();

        IList<String> list = instance.getList("list");
        assertTrueEventually(() -> assertEquals(rangeEnd - rangeStart + 1, list.size()));
        assertEquals(rangeStart + ":foobar-" + rangeStart , list.get(0));
        assertEquals(rangeEnd + ":foobar-" + rangeEnd , list.get(list.size() - 1));
    }

    private void fillSortedSet(String sortedSet, int elementCount) {
        RedisCommands<String, String> commands = connection.sync();

        int batchSize = 100_000;
        List<ScoredValue<String>> scoredValues = new ArrayList<>(batchSize);
        for (int i = 0; i < elementCount; i++) {
            scoredValues.add(ScoredValue.fromNullable(i, "foobar-" + i));
            if (scoredValues.size() == batchSize) {
                long added = commands.zadd(sortedSet, scoredValues.toArray(new ScoredValue[0]));
                assertEquals(scoredValues.size(), added);
                scoredValues.clear();
            }
        }
        if (!scoredValues.isEmpty()) {
            long added = commands.zadd(sortedSet, scoredValues.toArray(new ScoredValue[0]));
            assertEquals(scoredValues.size(), added);
        }
    }

    @Test
    public void stream() {
        int addCount = 500;
        int streamCount = 2;

        for (int i = 0; i < streamCount; i++) {
            fillStream("stream-" + i, addCount);
        }

        Map<String, String> streamOffsets = new HashMap<>();
        for (int i = 0; i < streamCount; i++) {
            streamOffsets.put("stream-" + i, "0");
        }

        Sink<Object> sink = SinkBuilder
                .sinkBuilder("set", c -> c.jetInstance().getHazelcastInstance().getSet("set"))
                .receiveFn(Set::add)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(RedisSources.stream("source", uri, streamOffsets,
                mes -> mes.getStream() + " - " + mes.getId()))
                .withoutTimestamps()
                .writeTo(sink);

        Job job = instance.newJob(p);

        Collection<Object> set = instance.getHazelcastInstance().getSet("set");
        assertTrueEventually(() -> assertEquals(addCount * streamCount, set.size()));

        job.cancel();
    }

    @Test
    public void streamFaultTolerance() {
        int addCount = 10_000;
        int streamCount = 4;

        for (int i = 0; i < streamCount; i++) {
            int streamIndex = i;
            spawn(() -> fillStream("stream-" + streamIndex, addCount));
        }

        Map<String, String> streamOffsets = new HashMap<>();
        for (int i = 0; i < streamCount; i++) {
            streamOffsets.put("stream-" + i, "0");
        }

        Sink<Object> sink = SinkBuilder
                .sinkBuilder("set", c -> c.jetInstance().getHazelcastInstance().getSet("set"))
                .receiveFn(Set::add)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(RedisSources.stream("source", uri, streamOffsets,
                mes -> mes.getStream() + " - " + mes.getId()))
                .withoutTimestamps()
                .writeTo(sink);


        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(3000);
        Job job = instance.newJob(p, config);

        sleepSeconds(15);
        instanceToShutDown.shutdown();
        sleepSeconds(15);
        instanceToShutDown = createJetMember();

        Collection<Object> set = instance.getHazelcastInstance().getSet("set");
        assertTrueEventually(() -> assertEquals(addCount * streamCount, set.size()));

        job.cancel();
    }

    private void fillStream(String stream, int addCount) {
        RedisCommands<String, String> commands = connection.sync();
        for (int i = 0; i < addCount; i++) {
            commands.xadd(stream, "foo-" + i, "bar-" + i);
        }
    }

}
