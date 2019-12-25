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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBResultMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static org.influxdb.InfluxDBFactory.connect;

/**
 * Contains factory methods for creating InfluxDB sources.
 */
public final class InfluxDbSources {

    private InfluxDbSources() {
    }

    /**
     * Creates a source that executes the query on given database and
     * emits items mapped with user defined mapper function.
     * Authenticates with the server using given credentials.
     *
     * Example pipeline which reads records from InfluxDb, maps the first two
     * columns to a tuple and logs them can be seen below: <pre>{@code
     *     Pipeline p = Pipeline.create();
     *     p.readFrom(
     *             InfluxDbSources.influxDb("SELECT * FROM db..cpu_usages",
     *                     DATABASE_NAME,
     *                     INFLUXDB_URL,
     *                     USERNAME,
     *                     PASSWORD,
     *                     (name, tags, columns, row) -> tuple2(row.get(0), row.get(1))))
     *     )
     *      .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param query                 query to execute on InfluxDb database
     * @param database              name of the database
     * @param url                   url of the InfluxDb server
     * @param username              username of the InfluxDb server
     * @param password              password of the InfluxDb server
     * @param measurementProjection a function which takes measurement name, tags set, column names and values
     *                              as argument and produces the user object {@link T} which will be emitted from
     *                              this source
     * @param <T>                   type of the user object
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(
            @Nonnull String query,
            @Nonnull String database,
            @Nonnull String url,
            @Nonnull String username,
            @Nullable String password,
            @Nonnull MeasurementProjection<T> measurementProjection
    ) {
        checkNotNull(query, "query cannot be null");
        checkNotNull(url, "url cannot be null");
        checkNotNull(database, "database cannot be null");
        checkNotNull(username, "username cannot be null");
        checkNotNull(measurementProjection, "measurementMapper cannot be null");

        return influxDb(query, () -> connect(url, username, password).setDatabase(database), measurementProjection);
    }

    /**
     * Creates a source that connects to InfluxDB database using the given
     * connection supplier and emits items mapped with the given mapper
     * function.
     *
     * Example pipeline which reads records from InfluxDb, maps the first two
     * columns to a tuple and logs them can be seen below: <pre>{@code
     *     Pipeline p = Pipeline.create();
     *     p.readFrom(
     *             InfluxDbSources.influxDb("SELECT * FROM db..cpu_usages",
     *                     () -> InfluxDBFactory.connect(url, username, password).setDatabase(database)
     *                     (name, tags, columns, row) -> tuple2(row.get(0), row.get(1))))
     *     )
     *      .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param <T>                   type of the user object
     * @param query                 query to execute on InfluxDb database
     * @param connectionSupplier    supplier which returns {@link InfluxDB} instance
     * @param measurementProjection mapper function which takes measurement name, tags set, column names and values
     *                              as argument and produces the user object {@link T} which will be emitted from
     *                              this source
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(
            @Nonnull String query,
            @Nonnull SupplierEx<InfluxDB> connectionSupplier,
            @Nonnull MeasurementProjection<T> measurementProjection
    ) {
        checkNotNull(query, "query cannot be null");
        checkNotNull(connectionSupplier, "connectionSupplier cannot be null");
        checkNotNull(measurementProjection, "connectionSupplier cannot be null");

        return SourceBuilder.batch("influxdb",
                ignored -> new InfluxDbSourceContext<>(query, connectionSupplier, null,
                        measurementProjection))
                .<T>fillBufferFn(InfluxDbSourceContext::fillBufferWithMeasurementMapping)
                .destroyFn(InfluxDbSourceContext::close)
                .build();
    }

    /**
     * Creates a source that executes a query on given database and
     * emits result which are mapped to the provided POJO class type.
     * Authenticates with the server using given credentials.
     *
     * Example pipeline which reads records from InfluxDb, maps them
     * to the provided POJO and logs them can be seen below: <pre>{@code
     *     Pipeline p = Pipeline.create();
     *     p.readFrom(
     *             InfluxDbSources.influxDb("SELECT * FROM db..cpu",
     *                         DATABASE_NAME,
     *                         INFLUXDB_URL,
     *                         USERNAME,
     *                         PASSWORD,
     *                         Cpu.class)
     *       )
     *      .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param query     query to execute on InfluxDb database
     * @param database  name of the database
     * @param url       url of the InfluxDb server
     * @param username  username of the InfluxDb server
     * @param password  password of the InfluxDb server
     * @param pojoClass the POJO class instance
     * @param <T>       the POJO class
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(
            @Nonnull String query,
            @Nonnull String database,
            @Nonnull String url,
            @Nonnull String username,
            String password,
            @Nonnull Class<T> pojoClass
    ) {
        checkNotNull(query, "query cannot be null");
        checkNotNull(url, "url cannot be null");
        checkNotNull(database, "database cannot be null");
        checkNotNull(username, "username cannot be null");
        checkNotNull(pojoClass, "pojoClass cannot be null");

        return influxDb(query, () -> connect(url, username, password).setDatabase(database), pojoClass);
    }

    /**
     * Creates a source that connects to InfluxDB database using the given
     * connection supplier and emits items which are mapped to the provided
     * POJO class type.
     *
     * Example pipeline which reads records from InfluxDb, maps them
     * to the provided POJO and logs them can be seen below: <pre>{@code
     *     Pipeline p = Pipeline.create();
     *     p.readFrom(
     *             InfluxDbSources.influxDb("SELECT * FROM db..cpu",
     *                       () -> InfluxDBFactory.connect(url, username, password).setDatabase(database)
     *                       Cpu.class)
     *       )
     *      .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param query              query to execute on InfluxDb database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param pojoClass          the POJO class instance
     * @param <T>                the POJO class
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom}
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(
            @Nonnull String query,
            @Nonnull SupplierEx<InfluxDB> connectionSupplier,
            @Nonnull Class<T> pojoClass
    ) {
        checkNotNull(query, "query cannot be null");
        checkNotNull(connectionSupplier, "username cannot be null");
        checkNotNull(pojoClass, "pojoClass cannot be null");

        return SourceBuilder.batch("influxdb",
                ignored -> new InfluxDbSourceContext<>(query, connectionSupplier, pojoClass, null))
                .<T>fillBufferFn(InfluxDbSourceContext::fillBufferWithPojoMapping)
                .destroyFn(InfluxDbSourceContext::close)
                .build();
    }

    /**
     * A source context around the InfluxDB connection that executes a query on
     * InfluxDb and streams the results.
     *
     * @param <T> emitted item type
     */
    private static class InfluxDbSourceContext<T> {

        /**
         * Default number of {@link QueryResult}s to process in one chunk
         */
        private static final int DEFAULT_CHUNK_SIZE = 100;
        private static final int MAX_FILL_ELEMENTS = 100;

        private final Class<T> pojoClass;
        private final BlockingQueue<QueryResult> queue = new ArrayBlockingQueue<>(1000);
        private final ArrayList<QueryResult> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final InfluxDBResultMapper resultMapper;
        private final MeasurementProjection<T> measurementProjection;
        private InfluxDB db;
        private volatile boolean finished;

        InfluxDbSourceContext(
                @Nonnull String query,
                @Nonnull SupplierEx<InfluxDB> connectionSupplier,
                @Nullable Class<T> pojoClass,
                @Nullable MeasurementProjection<T> measurementProjection
        ) {
            assert pojoClass != null ^ measurementProjection != null;
            this.pojoClass = pojoClass;
            this.resultMapper = pojoClass != null ? new InfluxDBResultMapper() : null;
            this.measurementProjection = measurementProjection;
            db = connectionSupplier.get();
            db.query(new Query(query),
                    DEFAULT_CHUNK_SIZE,
                    e -> {
                        try {
                            queue.put(e);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    },
                    () -> finished = true
            );
        }

        void fillBufferWithPojoMapping(SourceBuffer<T> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (QueryResult result : buffer) {
                boolean done = throwExceptionIfResultWithErrorOrNull(result);
                if (done) {
                    break;
                }
                for (T t : resultMapper.toPOJO(result, pojoClass)) {
                    sourceBuffer.add(t);
                }
            }
            buffer.clear();
            if (finished && queue.isEmpty()) {
                sourceBuffer.close();
            }
        }

        void fillBufferWithMeasurementMapping(SourceBuffer<T> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (QueryResult result : buffer) {
                boolean done = throwExceptionIfResultWithErrorOrNull(result);
                if (done) {
                    break;
                }
                for (Result internalResult : result.getResults()) {
                    if (internalResult != null && internalResult.getSeries() != null) {
                        for (Series s : internalResult.getSeries()) {
                            for (List<Object> objects : s.getValues()) {
                                sourceBuffer.add(
                                        measurementProjection.apply(s.getName(), s.getTags(), s.getColumns(), objects));
                            }
                        }
                    }
                }
            }
            buffer.clear();
            if (finished && queue.isEmpty()) {
                sourceBuffer.close();
            }
        }

        void close() {
            if (db != null) {
                db.close();
            }
        }
    }

    private static boolean throwExceptionIfResultWithErrorOrNull(final QueryResult queryResult) {
        if (queryResult == null) {
            throw new RuntimeException("InfluxDB returned null query result");
        }
        if (queryResult.getResults() == null && "DONE".equals(queryResult.getError())) {
            return true;
        }
        if (queryResult.getError() != null) {
            throw new RuntimeException("InfluxDB returned an error: " + queryResult.getError());
        }
        if (queryResult.getResults() == null) {
            throw new RuntimeException("InfluxDB returned null query results");
        }
        for (Result seriesResult : queryResult.getResults()) {
            if (seriesResult.getError() != null) {
                throw new RuntimeException("InfluxDB returned an error with Series: " + seriesResult.getError());
            }
        }
        return false;
    }
}
