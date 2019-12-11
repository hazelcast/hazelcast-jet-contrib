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

package com.hazelcast.jet.contrib.probabilistic;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.contrib.probabilistic.HashingSupport.hashingFn;
import static com.hazelcast.jet.contrib.probabilistic.HashingSupport.hashingServiceFactory;
import static com.hazelcast.jet.contrib.probabilistic.ProbabilisticAggregations.hyperLogLog;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class HyperLogLogTest {

    private JetInstance jet;

    @Before
    public void setup() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().getMapConfig("default")
                 .getEventJournalConfig()
                 .setEnabled(true)
                 .setCapacity(50_000);
        jet = Jet.newJetInstance(jetConfig);
    }

    @After
    public void tearDown() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test_hll_stream() {
        int actualCardinality = 1234;
        int totalItemCount = 50_000;
        double allowedErrorFactor = 0.05;
        String sourceMapName = "sourceMap";
        String targetMapName = "targetMap";


        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.mapJournal(sourceMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(Map.Entry::getValue)
                .mapUsingService(hashingServiceFactory(), hashingFn())
                .rollingAggregate(ProbabilisticAggregations.hyperLogLog())
                .writeTo(Sinks.mapWithUpdating(targetMapName, e -> 0, (current, n) -> n));
        jet.newJob(pipeline);

        IMap<Integer, Integer> sourceMap = jet.getMap(sourceMapName);
        populateSourceMap(sourceMap, actualCardinality, totalItemCount);

        IMap<Integer, Long> targetMap = jet.getMap(targetMapName);
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            Long estimatedCardinality = targetMap.get(0);
            assertNotNull(estimatedCardinality);
            assertEstimatedCardinalityPrecision(estimatedCardinality, actualCardinality, allowedErrorFactor);
        });
    }

    @Test
    public void test_hll_batch() {
        int actualCardinality = 1234;
        int totalItemCount = 50_000;
        double allowedErrorFactor = 0.05;
        String sourceListName = "sourceList";
        String targetListName = "targetList";

        IList<Integer> sourceList = jet.getList(sourceListName);
        populateSourceList(sourceList, actualCardinality, totalItemCount);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.list(sourceListName))
                .mapUsingService(hashingServiceFactory(), hashingFn())
                .aggregate(hyperLogLog())
                .writeTo(list(targetListName));
        jet.newJob(pipeline).join();

        long estimatedCardinality = jet.<Long>getList(targetListName).get(0);
        assertEstimatedCardinalityPrecision(estimatedCardinality, actualCardinality, allowedErrorFactor);
    }

    private static void assertEstimatedCardinalityPrecision(long estimatedCardinality, int targetCardinality,
                                                            double allowedError) {
        long lowerBound = (long) (targetCardinality * ((double) 1 - allowedError));
        long upperBound = (long) (targetCardinality * ((double) 1 + allowedError));
        assertTrue(estimatedCardinality >= lowerBound);
        assertTrue(estimatedCardinality <= upperBound);
    }

    private static void populateSourceList(List<Integer> sourceList, int targetCardinality, int totalItemCount) {
        List<Integer> uniqElements = IntStream.range(0, targetCardinality).boxed().collect(Collectors.toList());
        for (int i = 0; i < totalItemCount; i++) {
            Integer randomElement = uniqElements.get(ThreadLocalRandom.current().nextInt(targetCardinality));
            sourceList.add(randomElement);
        }
    }

    private static void populateSourceMap(Map<Integer, Integer> sourceMap, int targetCardinality, int totalItemCount) {
        List<Integer> uniqElements = IntStream.range(0, targetCardinality).boxed().collect(Collectors.toList());
        for (int i = 0; i < totalItemCount; i++) {
            Integer randomElement = uniqElements.get(ThreadLocalRandom.current().nextInt(targetCardinality));
            sourceMap.put(i, randomElement);
        }
    }
}
