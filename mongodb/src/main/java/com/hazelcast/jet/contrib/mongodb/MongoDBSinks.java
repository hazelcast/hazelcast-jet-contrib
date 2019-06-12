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
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains factory methods for MongoDB sinks.
 */
public final class MongoDBSinks {

    private MongoDBSinks() {
    }

    /**
     * Creates a sink which adds objects to the specified collection using the
     * specified MongoDB client.
     *
     * @param name               name of the created sink
     * @param connectionSupplier MongoDB client supplier
     * @param databaseFn         creates/obtains a database using the given client
     * @param collectionFn       creates/obtains a collection in the given database
     * @param destroyFn          called upon completion to release any resource
     * @param ordered            sets the option of {@link InsertManyOptions#ordered(boolean)}
     * @param bypassValidation   sets the option of {@link InsertManyOptions#bypassDocumentValidation(Boolean)}
     */
    public static <T> Sink<T> mongodb(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier,
            @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn,
            @Nonnull FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn,
            @Nonnull ConsumerEx<MongoClient> destroyFn,
            boolean ordered,
            boolean bypassValidation
    ) {
        return SinkBuilder
                .sinkBuilder(name, ctx -> {
                    MongoClient client = connectionSupplier.get();
                    MongoCollection<T> collection = collectionFn.apply(databaseFn.apply(client));
                    return new MongoSinkContext<>(client, collection, destroyFn, ordered, bypassValidation);
                })
                .<T>receiveFn(MongoSinkContext::addDocument)
                .flushFn(MongoSinkContext::flush)
                .destroyFn(MongoSinkContext::close)
                .build();
    }

    /**
     * Convenience for {@link #mongodb(String, SupplierEx, FunctionEx,
     * FunctionEx, ConsumerEx, boolean, boolean)}.
     */
    public static Sink<Document> mongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection
    ) {
        return mongodb(name,
                () -> MongoClients.create(connectionString),
                client -> client.getDatabase(database),
                db -> db.getCollection(collection),
                MongoClient::close, true, false);
    }

    private static class MongoSinkContext<T> {

        final MongoClient client;
        final MongoCollection<T> collection;
        final ConsumerEx<MongoClient> destroyFn;
        final InsertManyOptions insertManyOptions;

        List<T> documents;

        MongoSinkContext(
                MongoClient client,
                MongoCollection<T> collection,
                ConsumerEx<MongoClient> destroyFn,
                boolean ordered,
                boolean bypassValidation
        ) {
            this.client = client;
            this.collection = collection;
            this.destroyFn = destroyFn;
            this.insertManyOptions = new InsertManyOptions()
                    .ordered(ordered)
                    .bypassDocumentValidation(bypassValidation);

            documents = new ArrayList<>();
        }

        void addDocument(T document) {
            documents.add(document);
        }

        void flush() {
            collection.insertMany(documents, insertManyOptions);
            documents = new ArrayList<>();
        }

        void close() {
            destroyFn.accept(client);
        }
    }
}
