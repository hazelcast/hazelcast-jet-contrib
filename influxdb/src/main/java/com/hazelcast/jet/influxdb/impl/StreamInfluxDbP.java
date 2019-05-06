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

package com.hazelcast.jet.influxdb.impl;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.SupplierEx;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.impl.InfluxDBResultMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;


/**
 * A processor which executes a streaming query on InfluxDb and emits
 * results as they arrive.
 *
 * @param <T> emitted item type
 */
public class StreamInfluxDbP<T> extends AbstractProcessor {

    private final String query;
    private final String database;
    private final int chunkSize;
    private final SupplierEx<InfluxDB> connectionSupplier;
    private final Class<T> clazz;
    private final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>(10000);
    private final ArrayList<QueryResult> buffer = new ArrayList<>();
    private final InfluxDBResultMapper resultMapper;
    private InfluxDB db;
    private Traverser<Object> traverser;
    private volatile boolean finished;

    public StreamInfluxDbP(String query, String database, int chunkSize, SupplierEx<InfluxDB> connectionSupplier,
                           Class<T> clazz) {
        this.query = query;
        this.database = database;
        this.chunkSize = chunkSize;
        this.connectionSupplier = connectionSupplier;
        this.clazz = clazz;
        this.resultMapper = clazz != null ? new InfluxDBResultMapper() : null;
    }

    @Override
    protected void init(Context context) {
        db = connectionSupplier.get();
        db.query(new Query(query, database),
                chunkSize,
                queue::add,
                () -> finished = true
        );
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            if (queue.drainTo(buffer) == 0) {
                return finished;
            } else {
                traverser = traverseIterable(buffer).flatMap(this::getTraverser);
            }
        }

        if (emitFromTraverser(traverser)) {
            buffer.clear();
            traverser = null;
        }
        return false;
    }

    private Traverser<?> getTraverser(QueryResult result) {
        if (resultMapper != null) {
            if (!result.hasError()) {
                List<T> tList = resultMapper.toPOJO(result, clazz);
                return Traversers.traverseIterable(tList);
            }
            return Traversers.empty();
        } else {
            List<Result> results = result.getResults();
            if (results != null) {
                return Traversers.traverseStream(results.stream().flatMap(r -> {
                    if (r.hasError()) {
                        throw new IllegalStateException("Query returned error response: " + r.getError());
                    }
                    return r.getSeries() != null ? r.getSeries().stream() : Stream.empty();
                }));
            }
            return Traversers.empty();
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    public static <T> ProcessorSupplier supplier(String query, String database, int chunkSize,
                                                 SupplierEx<InfluxDB> connectionSupplier, Class<T> clazz) {
        return ProcessorSupplier.of(() -> new StreamInfluxDbP<>(query, database, chunkSize, connectionSupplier, clazz));
    }
}
