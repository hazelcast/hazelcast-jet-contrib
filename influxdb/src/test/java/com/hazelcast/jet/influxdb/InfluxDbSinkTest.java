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

package com.hazelcast.jet.influxdb;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.influxdb.dto.Point;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class InfluxDbSinkTest {

    @Test
    @Ignore("Connects to actual database")
    public void test_influxDbsink() {
        JetInstance jet = Jet.newJetInstance();

        IListJet<Integer> measurements = jet.getList("mem_usage");
        for (int i = 0; i < 16 * 1024; i++) {
            measurements.add(i);
        }

        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.list(measurements))
                .map(memUsage -> Point.measurement("mem_usage")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("value", memUsage)
                        .build())
                .drainTo(InfluxDbSinks.influxDb("http://localhost:8086", "trades", "root", "root"));

        jet.newJob(p).join();
        jet.shutdown();
    }
}
