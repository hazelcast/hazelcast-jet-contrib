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
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBResultMapper;

import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.influxdb.impl.StreamInfluxDbP.supplier;
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
    public static BatchSource<Series> influxDb(String query, String database, String url, String username,
                                               String password) {
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
    public static BatchSource<Series> influxDb(String query, String database, SupplierEx<InfluxDB> connectionSupplier) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");

        return SourceBuilder
                .batch("influxdb" + database, context -> connectionSupplier.get()
                )
                .<Series>fillBufferFn((influxDB, sourceBuffer) -> {
                    QueryResult result = influxDB.query(new Query(query, database));
                    if (result.hasError()) {
                        throw new IllegalStateException("Query returned error response: " + result.getError());
                    }
                    List<Result> results = result.getResults();
                    if (results != null) {
                        results.stream().flatMap(r -> {
                            if (r.hasError()) {
                                throw new IllegalStateException("Query returned error response: " + r.getError());
                            }
                            return r.getSeries() != null ? r.getSeries().stream() : Stream.empty();
                        }).forEach(sourceBuffer::add);
                    }
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
    public static <T> BatchSource<T> influxDb(String query, String database, String url, String username, String password,
                                              Class<T> clazz) {
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
    public static <T> BatchSource<T> influxDb(String query, String database, SupplierEx<InfluxDB> connectionSupplier,
                                              Class<T> clazz) {
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
    public static StreamSource<Series> streamInfluxDb(String query, String database, String url, String username,
                                                      String password, int chunkSize) {
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
    public static StreamSource<Series> streamInfluxDb(String query, String database,
                                                      SupplierEx<InfluxDB> connectionSupplier, int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "connectionSupplier cannot be null");

        return Sources.streamFromProcessor("influxdb-" + database,
                forceTotalParallelismOne(supplier(query, database, chunkSize, connectionSupplier, null))
        );
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
    public static <T> StreamSource<T> streamInfluxDb(String query, String database, String url, String username,
                                                     String password, Class<T> clazz, int chunkSize) {
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
    public static <T> StreamSource<T> streamInfluxDb(String query, String database,
                                                     SupplierEx<InfluxDB> connectionSupplier, Class<T> clazz,
                                                     int chunkSize) {
        checkTrue(query != null, "query cannot be null");
        checkTrue(database != null, "database cannot be null");
        checkTrue(connectionSupplier != null, "username cannot be null");
        checkTrue(clazz != null, "clazz cannot be null");

        return Sources.streamFromProcessor("influxdb-" + database,
                forceTotalParallelismOne(supplier(query, database, chunkSize, connectionSupplier, clazz))
        );
    }
}
