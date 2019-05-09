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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.influxdb.measurement.Cpu;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.InfluxDBContainer;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.influxdb.InfluxDbSources.DEFAULT_CHUNK_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
public class InfluxDbSourceTest extends JetTestSupport {

    private static final String DATABASE_NAME = "test_db";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final int VALUE_COUNT = 1024;

    @Rule
    public InfluxDBContainer influxdbContainer = new InfluxDBContainer<>()
            .withAuthEnabled(true)
            .withDatabase(DATABASE_NAME)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);

    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }


    @Test
    public void test_stream_influxDbSource_withMeasurementMapper() {
        InfluxDB db = influxdbContainer.getNewInfluxDB();
        fillData(db);

        Pipeline p = Pipeline.create();

        p.drawFrom(
                InfluxDbSources.influxDb("SELECT * FROM test_db..test",
                        DATABASE_NAME,
                        influxdbContainer.getUrl(),
                        USERNAME,
                        PASSWORD,
                        DEFAULT_CHUNK_SIZE,
                        (name, tags, columns, row) -> tuple2(row.get(0), row.get(1))))
         .withTimestamps(tuple2 -> Instant.parse(valueOf(tuple2.f0())).toEpochMilli(), 0)
         .peek()
         .drainTo(Sinks.list("results"));

        jet.newJob(p).join();

        assertEquals(VALUE_COUNT, jet.getList("results").size());
    }

    @Test
    public void test_stream_influxDbSource_withPOJOResultMapper() {
        InfluxDB db = influxdbContainer.getNewInfluxDB();
        fillCpuData(db);

        Pipeline p = Pipeline.create();

        p.drawFrom(
                InfluxDbSources.influxDb("SELECT * FROM test_db..cpu",
                        DATABASE_NAME,
                        influxdbContainer.getUrl(),
                        USERNAME,
                        PASSWORD,
                        Cpu.class,
                        DEFAULT_CHUNK_SIZE))
         .withTimestamps(cpu -> cpu.time.toEpochMilli(), 0)
         .peek()
         .drainTo(Sinks.list("results"));

        jet.newJob(p).join();

        assertEquals(VALUE_COUNT, jet.getList("results").size());
    }

    private void fillData(InfluxDB influxDB) {
        IntStream.range(0, VALUE_COUNT)
                 .forEach(value ->
                         influxDB.write(DATABASE_NAME,
                                 "autogen",
                                 Point.measurement("test")
                                      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                      .addField("value", value)
                                      .build()
                         )
                 );
    }

    private void fillCpuData(InfluxDB influxDB) {
        IntStream.range(0, VALUE_COUNT)
                 .forEach(value -> {
                             Cpu cpu = new Cpu("localhost", (double) value);
                             influxDB.write(DATABASE_NAME,
                                     "autogen",
                                     Point.measurementByPOJO(cpu.getClass())
                                          .addFieldsFromPOJO(cpu)
                                          .build()
                             );
                         }
                 );
    }

}
