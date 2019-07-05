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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * See {@link MongoDBSources#builder(String, SupplierEx)}
 *
 * @param <T> type of the queried items
 * @since 3.2
 */
public class MongoDBSourceBuilder<T> {

    private final String name;
    private final SupplierEx<? extends MongoClient> connectionSupplier;

    private FunctionEx<? super MongoClient, ? extends MongoDatabase> databaseFn;
    private FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> collectionFn;
    private ConsumerEx<? super MongoClient> destroyFn = ConsumerEx.noop();

    /**
     * See {@link MongoDBSources#builder(String, SupplierEx)}
     */
    MongoDBSourceBuilder(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        this.name = name;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * @param databaseFn creates/obtains a database using the given client.
     */
    public MongoDBSourceBuilder<T> databaseFn(
            @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
    ) {
        checkSerializable(databaseFn, "databaseFn");
        this.databaseFn = databaseFn;
        return this;
    }

    /**
     * @param collectionFn creates/obtains a collection in the given database.
     */
    public MongoDBSourceBuilder<T> collectionFn(
            @Nonnull FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn
    ) {
        checkSerializable(collectionFn, "collectionFn");
        this.collectionFn = collectionFn;
        return this;
    }

    /**
     * @param destroyFn destroys the client.
     */
    public MongoDBSourceBuilder<T> destroyFn(@Nonnull ConsumerEx<MongoClient> destroyFn) {
        checkSerializable(destroyFn, "destroyFn");
        this.destroyFn = destroyFn;
        return this;
    }

    /**
     * Creates and returns the MongoDB {@link BatchSource} with the components
     * you supplied to this builder.
     */
    public <U> BatchSource<U> batch(
            @Nonnull FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn,
            @Nonnull FunctionEx<? super T, U> mapFn
    ) {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");
        checkNotNull(databaseFn, "databaseFn must be set");
        checkNotNull(collectionFn, "collectionFn must be set");

        checkSerializable(searchFn, "searchFn");
        checkSerializable(mapFn, "mapFn");

        SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
        FunctionEx<? super MongoClient, ? extends MongoDatabase> localDatabaseFn = databaseFn;
        FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> localCollectionFn = collectionFn;
        ConsumerEx<? super MongoClient> localDestroyFn = destroyFn;

        return SourceBuilder
                .batch(name, ctx -> {
                    MongoClient client = localConnectionSupplier.get();
                    MongoCollection<? extends T> collection = localCollectionFn.apply(localDatabaseFn.apply(client));
                    return new BatchContext<>(client, collection, searchFn, mapFn, localDestroyFn);
                })
                .<U>fillBufferFn(BatchContext::fillBuffer)
                .destroyFn(BatchContext::close)
                .build();
    }

    /**
     * Creates and returns the MongoDB {@link StreamSource} which watches the
     * collection supplied to this builder.
     */
    public <U> StreamSource<U> stream(
            @Nonnull FunctionEx<? super MongoCollection<? extends T>, ? extends ChangeStreamIterable<? extends T>>
                    searchFn,
            @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn
    ) {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");
        checkNotNull(databaseFn, "databaseFn must be set");
        checkNotNull(collectionFn, "collectionFn must be set");

        checkSerializable(searchFn, "searchFn");
        checkSerializable(mapFn, "mapFn");

        SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
        FunctionEx<? super MongoClient, ? extends MongoDatabase> localDatabaseFn = databaseFn;
        FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> localCollectionFn = collectionFn;
        ConsumerEx<? super MongoClient> localDestroyFn = destroyFn;

        return SourceBuilder
                .timestampedStream(name, ctx -> {
                    MongoClient client = localConnectionSupplier.get();
                    MongoCollection<? extends T> collection = localCollectionFn.apply(localDatabaseFn.apply(client));
                    return new StreamContext<>(client, searchFn.apply(collection).iterator(), mapFn, localDestroyFn);
                })
                .<U>fillBufferFn(StreamContext::fillBuffer)
                .destroyFn(StreamContext::close)
                .build();

    }

    /**
     * Creates and returns the MongoDB {@link StreamSource} which watches all
     * collections in the database supplied to this builder.
     */
    public <U> StreamSource<U> streamDatabase(
            @Nonnull FunctionEx<? super MongoDatabase, ? extends ChangeStreamIterable<? extends T>>
                    searchFn,
            @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn
    ) {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");
        checkNotNull(databaseFn, "databaseFn must be set");

        checkSerializable(searchFn, "searchFn");
        checkSerializable(mapFn, "mapFn");

        SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
        FunctionEx<? super MongoClient, ? extends MongoDatabase> localDatabaseFn = databaseFn;
        ConsumerEx<? super MongoClient> localDestroyFn = destroyFn;

        return SourceBuilder
                .timestampedStream(name, ctx -> {
                    MongoClient client = localConnectionSupplier.get();
                    MongoDatabase database = localDatabaseFn.apply(client);
                    return new StreamContext<>(client, searchFn.apply(database).iterator(), mapFn, localDestroyFn);
                })
                .<U>fillBufferFn(StreamContext::fillBuffer)
                .destroyFn(StreamContext::close)
                .build();

    }

    /**
     * Creates and returns the MongoDB {@link StreamSource} which watches all
     * collections accross all databases.
     */
    public <U> StreamSource<U> streamAll(
            @Nonnull FunctionEx<? super MongoClient, ? extends ChangeStreamIterable<? extends T>>
                    searchFn,
            @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn
    ) {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");

        checkSerializable(searchFn, "searchFn");
        checkSerializable(mapFn, "mapFn");

        SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
        ConsumerEx<? super MongoClient> localDestroyFn = destroyFn;

        return SourceBuilder
                .timestampedStream(name, ctx -> {
                    MongoClient client = localConnectionSupplier.get();
                    return new StreamContext<>(client, searchFn.apply(client).iterator(), mapFn, localDestroyFn);
                })
                .<U>fillBufferFn(StreamContext::fillBuffer)
                .destroyFn(StreamContext::close)
                .build();

    }

    private static class BatchContext<T, U> {

        private static final int BATCH_SIZE = 500;

        final MongoClient client;
        final FunctionEx<? super T, U> mapFn;
        final ConsumerEx<? super MongoClient> destroyFn;

        final MongoCursor<? extends T> cursor;

        BatchContext(
                MongoClient client,
                MongoCollection<? extends T> collection,
                FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn,
                FunctionEx<? super T, U> mapFn,
                ConsumerEx<? super MongoClient> destroyFn
        ) {
            this.client = client;
            this.mapFn = mapFn;
            this.destroyFn = destroyFn;

            cursor = searchFn.apply(collection).iterator();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<U> buffer) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (cursor.hasNext()) {
                    U item = mapFn.apply(cursor.next());
                    if (item != null) {
                        buffer.add(item);
                    }
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

    private static class StreamContext<T, U> {

        final MongoClient client;
        final FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn;
        final ConsumerEx<? super MongoClient> destroyFn;

        MongoCursor<? extends ChangeStreamDocument<? extends T>> cursor;

        StreamContext(
                MongoClient client,
                MongoCursor<? extends ChangeStreamDocument<? extends T>> cursor,
                FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
                ConsumerEx<? super MongoClient> destroyFn
        ) {
            this.client = client;
            this.mapFn = mapFn;
            this.destroyFn = destroyFn;

            this.cursor = cursor;
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<U> buffer) {
            ChangeStreamDocument<? extends T> changeStreamDocument = cursor.tryNext();
            if (changeStreamDocument != null) {
                long clusterTime = clusterTime(changeStreamDocument);
                U item = mapFn.apply(changeStreamDocument);
                if (item != null) {
                    buffer.add(item, clusterTime);
                }
            }
        }

        long clusterTime(ChangeStreamDocument<? extends T> changeStreamDocument) {
            BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
            return clusterTime == null ? System.currentTimeMillis() : clusterTime.getValue();
        }

        void close() {
            cursor.close();
            destroyFn.accept(client);
        }
    }


}
