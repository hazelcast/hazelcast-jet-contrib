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
import com.hazelcast.jet.pipeline.StreamSource;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static org.influxdb.InfluxDBFactory.connect;

/**
 * Contains factory methods for creating InfluxDB sources
 */
public final class InfluxDbSources {

    /**
     * Default number of {@link QueryResult}s to process in one chunk
     */
    public static final int DEFAULT_CHUNK_SIZE = 100;

    private InfluxDbSources() {

    }

    /**
     * Creates a source that executes given query on given database and
     * emits result series. Authenticates with the server using given credentials.
     *
     * @param query    query to execute on InfluxDb database
     * @param database name of the database
     * @param url      url of the InfluxDb server
     * @param username username of the InfluxDb server
     * @param password password of the InfluxDb server
     * @return {@link Series} which contains query results
     */
    @Nonnull
    public static BatchSource<Series> influxDb(@Nonnull String query, @Nonnull String database, @Nonnull String url,
                                               @Nonnull String username, @Nonnull String password) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(url != null, "url cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(username != null, "username cannot be null");
        checkTrue(password != null, "password cannot be null");
        return influxDb(query, database, () -> connect(url, username, password).setDatabase(database));
    }

    /**
     * Creates a source that executes given query on given database and
     * emits result series. Uses given {@link InfluxDB} instance to interact
     * with the server.
     *
     * @param query              query to execute on InfluxDb database
     * @param database           name of the database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @return {@link Series} which contains query results
     */
    @Nonnull
    public static BatchSource<Series> influxDb(@Nonnull String query, @Nonnull String database,
                                               @Nonnull SupplierEx<InfluxDB> connectionSupplier) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");

        return SourceBuilder
                .batch("influxdb" + database, context -> connectionSupplier.get())
                .<Series>fillBufferFn((influxDB, sourceBuffer) -> {
                    QueryResult result = influxDB.query(new Query(query, database));
                    if (result.hasError()) {
                        throw new IllegalStateException("Query returned error response: " + result.getError());
                    }
                    addResultsToSourceBuffer(result, sourceBuffer);
                    sourceBuffer.close();
                })
                .destroyFn(InfluxDB::close)
                .build();
    }

    /**
     * Creates a source that executes given query on given database, emits items
     * which are mapped to the provided POJO class type. Authenticates with the
     * server using given credentials.
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
        return influxDb(query, database, () -> connect(url, username, password).setDatabase(database), clazz);
    }

    /**
     * Creates a source that executes given query on given database, emits items
     * which are mapped to the provided POJO class type. Uses given {@link InfluxDB}
     * instance to interact with the server.
     *
     * @param query              query to execute on InfluxDb database
     * @param database           name of the database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param clazz              name of the POJO class
     * @param <T>                type of the POJO class
     * @return <T> emits instances of POJO type
     */
    @Nonnull
    public static <T> BatchSource<T> influxDb(@Nonnull String query, @Nonnull String database,
                                              @Nonnull SupplierEx<InfluxDB> connectionSupplier, @Nonnull Class<T> clazz) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return SourceBuilder
                .batch("influxdb-" + database, context -> connectionSupplier.get())
                .<T>fillBufferFn((influxDB, sourceBuffer) -> {
                    QueryResult result = influxDB.query(new Query(query, database));
                    if (result.hasError()) {
                        throw new IllegalStateException("Query returned error response: " + result.getError());
                    }
                    new InfluxDBResultMapper().toPOJO(result, clazz).forEach(sourceBuffer::add);
                    sourceBuffer.close();
                })
                .destroyFn(InfluxDB::close)
                .build();
    }

    /**
     * Creates a source that executes a streaming query on given database and
     * emits result series in a streaming fashion. Authenticates with the server
     * using given credentials.
     *
     * @param query     query to execute on InfluxDb database
     * @param database  name of the database
     * @param url       url of the InfluxDb server
     * @param username  username of the InfluxDb server
     * @param password  password of the InfluxDb server
     * @param chunkSize the number of {@link QueryResult}s to process in one chunk.
     * @return {@link Series} which contains query results
     */
    @Nonnull
    public static StreamSource<Series> streamInfluxDb(@Nonnull String query, @Nonnull String database, @Nonnull String url,
                                                      @Nonnull String username, @Nonnull String password, int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(url != null, "url cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(username != null, "username cannot be null");
        checkTrue(password != null, "password cannot be null");

        return streamInfluxDb(query, database, () -> connect(url, username, password).setDatabase(database), chunkSize);
    }

    /**
     * Creates a source that executes a streaming query on given database and
     * emits result series in a streaming fashion. Uses given {@link InfluxDB}
     * instance to interact with the server.
     *
     * @param query              query to execute on InfluxDb database
     * @param database           name of the database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param chunkSize          the number of {@link QueryResult}s to process in one chunk.
     * @return {@link Series} which contains query results
     */
    @Nonnull
    public static StreamSource<Series> streamInfluxDb(@Nonnull String query, @Nonnull String database,
                                                      @Nonnull SupplierEx<InfluxDB> connectionSupplier, int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");

        return SourceBuilder.stream("influxdb-" + database,
                ignored -> new InfluxDbStreamingSource<>(query, database, chunkSize, connectionSupplier, null))
                .<Series>fillBufferFn(InfluxDbStreamingSource::addToBufferWithOutMapping)
                .destroyFn(InfluxDbStreamingSource::close)
                .build();
    }

    /**
     * Creates a source that executes a streaming query on given database and
     * emits result which are mapped to the provided POJO class type in a
     * streaming fashion. Authenticates with the server using given credentials.
     *
     * @param query     query to execute on InfluxDb database
     * @param database  name of the database
     * @param url       url of the InfluxDb server
     * @param username  username of the InfluxDb server
     * @param password  password of the InfluxDb server
     * @param clazz     name of the POJO class
     * @param <T>       type of the POJO class
     * @param chunkSize the number of {@link QueryResult}s to process in one chunk.
     * @return <T> emits instances of POJO type
     */
    @Nonnull
    public static <T> StreamSource<T> streamInfluxDb(@Nonnull String query, @Nonnull String database, @Nonnull String url,
                                                     @Nonnull String username, @Nonnull String password,
                                                     @Nonnull Class<T> clazz, int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(url != null, "url cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(username != null, "username cannot be null");
        checkTrue(password != null, "password cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return streamInfluxDb(query, database, () -> connect(url, username, password).setDatabase(database),
                clazz, chunkSize);
    }

    /**
     * Creates a source that executes a streaming query on given database and
     * emits result which are mapped to the provided POJO class type in a
     * streaming fashion. Uses given {@link InfluxDB} instance to interact with
     * the server.
     *
     * @param query              query to execute on InfluxDb database
     * @param database           name of the database
     * @param connectionSupplier supplier which returns {@link InfluxDB} instance
     * @param clazz              name of the POJO class
     * @param <T>                type of the POJO class
     * @param chunkSize          the number of {@link QueryResult}s to process in one chunk.
     * @return <T> emits instances of POJO type
     */
    @Nonnull
    public static <T> StreamSource<T> streamInfluxDb(@Nonnull String query, @Nonnull String database,
                                                     @Nonnull SupplierEx<InfluxDB> connectionSupplier,
                                                     @Nonnull Class<T> clazz, int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "username cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return SourceBuilder.timestampedStream("influxdb-" + database,
                ignored -> new InfluxDbStreamingSource<>(query, database, chunkSize, connectionSupplier, clazz))
                .<T>fillBufferFn(InfluxDbStreamingSource::addToBufferWithMapping)
                .destroyFn(InfluxDbStreamingSource::close)
                .build();
    }

    /**
     * A streaming source which executes a streaming query on InfluxDb and emits
     * results as they arrive.
     *
     * @param <T> emitted item type
     */
    private static class InfluxDbStreamingSource<T> {

        private final Class<T> clazz;
        private final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>(10000);
        private final ArrayList<QueryResult> buffer = new ArrayList<>();
        private final InfluxDBResultMapper resultMapper;
        private InfluxDB db;
        private volatile boolean finished;

        InfluxDbStreamingSource(@Nonnull String query, @Nonnull String database, int chunkSize,
                                @Nonnull SupplierEx<InfluxDB> connectionSupplier, @Nullable Class<T> clazz) {
            this.clazz = clazz;
            this.resultMapper = clazz != null ? new InfluxDBResultMapper() : null;
            db = connectionSupplier.get();
            db.query(new Query(query, database),
                    chunkSize,
                    queue::add,
                    () -> finished = true
            );
        }

        void addToBufferWithMapping(SourceBuffer<T> sourceBuffer) {
            transferTo(result -> {
                if (!result.hasError()) {
                    resultMapper.toPOJO(result, clazz).forEach(sourceBuffer::add);
                }
            }, sourceBuffer);
        }

        void addToBufferWithOutMapping(SourceBuffer<Series> sourceBuffer) {
            transferTo(result -> addResultsToSourceBuffer(result, sourceBuffer), sourceBuffer);
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

    private static void addResultsToSourceBuffer(QueryResult result, SourceBuffer<Series> sourceBuffer) {
        List<Result> results = result.getResults();
        if (results != null) {
            results.stream().flatMap(r -> {
                if (r.hasError()) {
                    throw new IllegalStateException("Query returned error response: " + r.getError());
                }
                return r.getSeries() != null ? r.getSeries().stream() : Stream.empty();
            }).forEach(sourceBuffer::add);
        }
    }
}
