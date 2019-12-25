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

package com.hazelcast.jet.contrib.influxdb;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class InfluxDbSinkTest extends JetTestSupport {

    private static final String DATABASE_NAME = "test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String SERIES = "mem_usage";
    private static final int VALUE_COUNT = 16 * 1024;

    @Rule
    public InfluxDBContainer influxdbContainer = new InfluxDBContainer<>()
            .withAuthEnabled(true)
            .withDatabase(DATABASE_NAME)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withNetwork(Network.newNetwork());

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @Test
    public void test_influxDbSink() {
        IList<Integer> measurements = jet.getList("mem_usage");
        for (int i = 0; i < VALUE_COUNT; i++) {
            measurements.add(i);
        }

        InfluxDB db = influxdbContainer.getNewInfluxDB();
        db.query(new Query("DROP SERIES FROM mem_usage"));

        Pipeline p = Pipeline.create();

        int startTime = 0;
        p.readFrom(Sources.list(measurements))
         .map(index -> Point.measurement("mem_usage")
                            .time(startTime + index, TimeUnit.MILLISECONDS)
                            .addField("value", index)
                            .build())
         .writeTo(InfluxDbSinks.influxDb(influxdbContainer.getUrl(), DATABASE_NAME, USERNAME, PASSWORD));

        jet.newJob(p).join();

        List<Result> results = db.query(new Query("SELECT * FROM mem_usage")).getResults();
        assertEquals(1, results.size());
        List<Series> seriesList = results.get(0).getSeries();
        assertEquals(1, seriesList.size());
        Series series = seriesList.get(0);
        assertEquals(SERIES, series.getName());
        assertEquals(VALUE_COUNT, series.getValues().size());
    }

    @Test
    public void test_influxDbSink_nonExistingDb() {
        IList<Integer> measurements = jet.getList("mem_usage");
        IntStream.range(0, VALUE_COUNT).forEach(measurements::add);
        influxdbContainer.getNewInfluxDB();

        Pipeline p = Pipeline.create();
        int startTime = 0;
        p.readFrom(Sources.list(measurements))
         .map(index -> Point.measurement("mem_usage")
                            .time(startTime + index, TimeUnit.MILLISECONDS)
                            .addField("value", index)
                            .build())
         .writeTo(InfluxDbSinks.influxDb(influxdbContainer.getUrl(), "non-existing", USERNAME, PASSWORD));

        expected.expectMessage("database not found: \"non-existing\"");
        jet.newJob(p).join();
    }
}
