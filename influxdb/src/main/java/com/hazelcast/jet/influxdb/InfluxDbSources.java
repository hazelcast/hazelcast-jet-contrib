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

import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import mapper.MeasurementMapper;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBResultMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Objects.nonNull;
import static org.influxdb.InfluxDBFactory.connect;

/**
 * Contains factory methods for creating InfluxDB sources
 */
public final class InfluxDbSources {

    private InfluxDbSources() {

    }

    /**
     * Creates a source that executes the query on given database and
     * emits items mapped with user defined mapper function.
     * Authenticates with the server using given credentials.
     *
     * @param <T>               type of the user object
     * @param query             query to execute on InfluxDb database
     * @param database          name of the database
     * @param url               url of the InfluxDb server
     * @param username          username of the InfluxDb server
     * @param password          password of the InfluxDb server
     * @param measurementMapper mapper function which takes measurement name, tags set, column names and values
     *                          as argument and produces the user object {@link T} which will be emitted from
     *                          this source
     * @return {@link T} mapped user objects
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(@Nonnull String query, @Nonnull String database, @Nonnull String url,
                                              @Nonnull String username, @Nonnull String password,
                                              @Nonnull MeasurementMapper<T> measurementMapper) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(url != null, "url cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(username != null, "username cannot be null");
        checkTrue(password != null, "password cannot be null");
        checkTrue(measurementMapper != null, "measurementMapper cannot be null");

        return influxDb(query, () -> connect(url, username, password).setDatabase(database), measurementMapper);
    }

    /**
     * Creates a source that executes the query on given database and
     * emits items mapped with user defined mapper function.
     * Uses given {@link InfluxDB} instance to interact with the server.
     *
     * @param <T>                type of the user object
     * @param query              query to execute on InfluxDb database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param measurementMapper  mapper function which takes measurement name, tags set, column names and values
     *                           as argument and produces the user object {@link T} which will be emitted from
     *                           this source
     * @return {@link T} mapped user objects
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(@Nonnull String query,
                                              @Nonnull SupplierEx<InfluxDB> connectionSupplier,
                                              @Nonnull MeasurementMapper<T> measurementMapper) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");
        checkTrue(measurementMapper != null, "connectionSupplier cannot be null");

        return SourceBuilder.batch("influxdb",
                ignored -> new InfluxDbSource<>(query, connectionSupplier, null,
                        measurementMapper))
                .<T>fillBufferFn(InfluxDbSource::addToBufferWithMeasurementMapping)
                .destroyFn(InfluxDbSource::close)
                .build();
    }

    /**
     * Creates a source that executes a query on given database and
     * emits result which are mapped to the provided POJO class type.
     * Authenticates with the server using given credentials.
     *
     * @param query    query to execute on InfluxDb database
     * @param database name of the database
     * @param url      url of the InfluxDb server
     * @param username username of the InfluxDb server
     * @param password password of the InfluxDb server
     * @param clazz    name of the POJO class
     * @param <T>      type of the POJO class
     * @return <T> emits instances of POJO type
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(@Nonnull String query, @Nonnull String database, @Nonnull String url,
                                              @Nonnull String username, @Nonnull String password,
                                              @Nonnull Class<T> clazz) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(url != null, "url cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(username != null, "username cannot be null");
        checkTrue(password != null, "password cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return influxDb(query, () -> connect(url, username, password).setDatabase(database), clazz);
    }

    /**
     * Creates a source that executes a query on given database and
     * emits result which are mapped to the provided POJO class type.
     * Uses given {@link InfluxDB} instance to interact with the server.
     *
     * @param query              query to execute on InfluxDb database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param clazz              name of the POJO class
     * @param <T>                type of the POJO class
     * @return <T> emits instances of POJO type
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(@Nonnull String query,
                                              @Nonnull SupplierEx<InfluxDB> connectionSupplier,
                                              @Nonnull Class<T> clazz) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(connectionSupplier != null, "username cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return SourceBuilder.batch("influxdb",
                ignored -> new InfluxDbSource<>(query, connectionSupplier, clazz, null))
                .<T>fillBufferFn(InfluxDbSource::addToBufferWithPOJOMapping)
                .destroyFn(InfluxDbSource::close)
                .build();
    }

    /**
     * A streaming source which executes a query on InfluxDb and emits
     * results as they arrive.
     *
     * @param <T> emitted item type
     */
    private static class InfluxDbSource<T> {

        /**
         * Default number of {@link QueryResult}s to process in one chunk
         */
        private static final int DEFAULT_CHUNK_SIZE = 100;

        private final Class<T> clazz;
        private final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>(10000);
        private final ArrayList<QueryResult> buffer = new ArrayList<>();
        private final InfluxDBResultMapper resultMapper;
        private final MeasurementMapper<T> measurementMapper;
        private InfluxDB db;
        private volatile boolean finished;

        InfluxDbSource(@Nonnull String query,
                       @Nonnull SupplierEx<InfluxDB> connectionSupplier, @Nullable Class<T> clazz,
                       @Nullable MeasurementMapper<T> measurementMapper) {
            this.clazz = clazz;
            this.resultMapper = clazz != null ? new InfluxDBResultMapper() : null;
            this.measurementMapper = measurementMapper;
            db = connectionSupplier.get();
            db.query(new Query(query),
                    DEFAULT_CHUNK_SIZE,
                    queue::add,
                    () -> finished = true
            );
        }

        void addToBufferWithPOJOMapping(SourceBuffer<T> sourceBuffer) {
            transferTo(result -> {
                throwExceptionIfResultWithErrorOrNull(result);
                if (!result.hasError()) {
                    resultMapper.toPOJO(result, clazz)
                                .forEach(sourceBuffer::add);
                }
            }, sourceBuffer);
        }

        void addToBufferWithMeasurementMapping(SourceBuffer<T> sourceBuffer) {
            transferTo(result -> {
                throwExceptionIfResultWithErrorOrNull(result);
                if (!result.hasError()) {
                    result.getResults()
                          .stream()
                          .filter(internalResult -> nonNull(internalResult) && nonNull(internalResult.getSeries()))
                          .flatMap(expandResultsToRows())
                          .forEach(sourceBuffer::add);
                }
            }, sourceBuffer);
        }

        private Function<Result, Stream<? extends T>> expandResultsToRows() {
            return r -> r.getSeries()
                         .stream()
                         .flatMap(expandSeries());
        }

        private Function<Series, Stream<? extends T>> expandSeries() {
            return s -> s.getValues()
                         .stream()
                         .map(objects -> measurementMapper.apply(s.getName(), s.getTags(), s.getColumns(), objects));
        }

        private <B> void transferTo(Consumer<QueryResult> consumer, SourceBuffer<B> sourceBuffer) {
            queue.drainTo(buffer);
            buffer.forEach(consumer);
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


    private static void throwExceptionIfResultWithErrorOrNull(final QueryResult queryResult) {
        if (queryResult == null) {
            throw new RuntimeException("InfluxDB returned null query result");
        }
        if (queryResult.getResults() == null && "DONE".equals(queryResult.getError())) {
            return;
        }
        if (queryResult.getError() != null) {
            throw new RuntimeException("InfluxDB returned an error: " + queryResult.getError());
        }
        if (queryResult.getResults() == null) {
            throw new RuntimeException("InfluxDB returned null query result");
        }
        queryResult.getResults().forEach(seriesResult -> {
            if (seriesResult.getError() != null) {
                throw new RuntimeException("InfluxDB returned an error with Series: " + seriesResult.getError());
            }
        });
    }

}
