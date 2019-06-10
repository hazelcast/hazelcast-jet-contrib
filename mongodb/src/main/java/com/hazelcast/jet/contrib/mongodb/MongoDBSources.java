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

package com.hazelcast.jet.contrib.mongodb;

import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains factory methods for MongoDB sources
 */
public final class MongoDBSources {

    private MongoDBSources() {
    }


    /**
     * @param name               name of the created source
     * @param connectionSupplier MongoDB client supplier
     * @param databaseFn         creates/obtains a database using the given client
     * @param collectionFn       creates/obtains a collection in the given database
     * @param searchFn           queries the collection and returns a {@link FindIterable}
     * @param destroyFn          called upon completion to release any resource
     */
    public static <T, U> BatchSource<U> mongodb(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier,
            @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn,
            @Nonnull FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn,
            @Nonnull FunctionEx<MongoCollection<T>, MongoIterable<T>> searchFn,
            @Nonnull FunctionEx<T, U> mapper,
            @Nonnull ConsumerEx<MongoClient> destroyFn
    ) {
        return SourceBuilder
                .batch(name, ctx -> {
                    MongoClient client = connectionSupplier.get();
                    MongoCollection<T> collection = collectionFn.apply(databaseFn.apply(client));
                    return new MongoSourceContext<>(client, collection, searchFn, mapper, destroyFn);
                })
                .<U>fillBufferFn(MongoSourceContext::fillBuffer)
                .destroyFn(MongoSourceContext::close)
                .build();
    }

    /**
     * Convenience for {@link #mongodb(String, SupplierEx, FunctionEx,
     * FunctionEx, FunctionEx, FunctionEx, ConsumerEx)}
     */
    public static BatchSource<Document> mongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return mongodb(name,
                () -> MongoClients.create(connectionString),
                client -> client.getDatabase(database),
                db -> db.getCollection(collection),
                col -> col.find().filter(filter).projection(projection),
                t -> t,
                MongoClient::close);
    }

    /**
     * todo
     */
    public static <T, U> StreamSource<U> streamMongodb(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier,
            @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn,
            @Nonnull FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn,
            @Nonnull FunctionEx<MongoCollection<T>, ? extends MongoIterable<T>> searchFn,
            @Nonnull FunctionEx<T, U> mapper,
            @Nonnull ConsumerEx<MongoClient> destroyFn
    ) {
        return SourceBuilder
                .stream(name, ctx -> {
                    MongoClient client = connectionSupplier.get();
                    MongoCollection<T> collection = collectionFn.apply(databaseFn.apply(client));
                    return new MongoSourceContext<>(client, collection, searchFn, mapper, destroyFn);
                })
                .<U>fillBufferFn(MongoSourceContext::fillBuffer)
                .destroyFn(MongoSourceContext::close)
                .build();
    }

    /**
     * Convenience for {@link #streamMongodb(String, SupplierEx, FunctionEx,
     * FunctionEx, FunctionEx, FunctionEx, ConsumerEx)}
     */
    public static StreamSource<Document> streamMongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return streamMongodb(name,
                () -> MongoClients.create(connectionString),
                client -> client.getDatabase(database),
                db -> db.getCollection(collection),
                col -> {
                    List<Bson> aggregates = new ArrayList<>();
                    if (filter != null) {
                        aggregates.add(Aggregates.match(filter));
                    }
                    if (projection != null) {
                        aggregates.add(Aggregates.project(projection));
                    }
                    ChangeStreamIterable<Document> watch = null;
                    if (aggregates.isEmpty()) {
                        watch = col.watch();
                    } else {
                        watch = col.watch(aggregates);
                    }
                    return watch.map(ChangeStreamDocument::getFullDocument);
                },
                t -> t,
                MongoClient::close);
    }

    private static class MongoSourceContext<T, U> {

        private static final int BATCH_SIZE = 500;

        final MongoClient client;
        final FunctionEx<T, U> mapper;
        final ConsumerEx<MongoClient> destroyFn;

        final MongoCursor<T> cursor;

        MongoSourceContext(
                MongoClient client,
                MongoCollection<T> collection,
                FunctionEx<MongoCollection<T>, ? extends MongoIterable<T>> searchFn,
                FunctionEx<T, U> mapper,
                ConsumerEx<MongoClient> destroyFn
        ) {
            this.client = client;
            this.mapper = mapper;
            this.destroyFn = destroyFn;

            cursor = searchFn.apply(collection).iterator();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<U> buffer) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (cursor.hasNext()) {
                    buffer.add(mapper.apply(cursor.next()));
                } else {
                    buffer.close();
                }
            }
        }

        void close() {
            cursor.close();
            destroyFn.accept(client);
        }
    }
}
