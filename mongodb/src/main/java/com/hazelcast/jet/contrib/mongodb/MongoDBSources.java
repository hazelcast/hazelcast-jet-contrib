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

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.function.FunctionEx.identity;

/**
 * Contains factory methods for MongoDB sources
 */
public final class MongoDBSources {

    private MongoDBSources() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link BatchSource} or {@link StreamSource} for the
     * Pipeline API.
     * <p>
     * These are the callback functions you can provide to implement the
     * source's behavior:
     * <ol><li>
     *     {@code connectionSupplier} supplies MongoDb client. It will be called
     *     once for each worker thread. This component is required.
     * </li><li>
     *     {@code databaseFn} creates/obtains a database using the given client.
     *     It will be called once for each worker thread. This component is
     *     required.
     * </li><li>
     *     {@code collectionFn} creates/obtains a collection in the given
     *     database. It will be called once for each worker thread. This
     *     component is required.
     * </li><li>
     *     {@code destroyFn} destroys the client. It will be called upon
     *     completion to release any resource. This component is optional.
     * </li></ol>
     * <p>
     * To create a MongoDB {@link BatchSource} use
     * {@link MongoDBSourceBuilder#batch(FunctionEx, FunctionEx)} by providing
     * a {@code searchFn} which will query the collection and a {@code mapFn}
     * which will transform the queried items to the desired output items.
     *
     * To create a MongoDB {@link StreamSource} use
     * {@link MongoDBSourceBuilder#stream(FunctionEx, FunctionEx)} by providing
     * a {@code searchFn} which will watch the changes in the collection and a
     * {@code mapFn} which will transform the changes to the desired output
     * items.
     *
     * If the {@code mapFn} returns a {@code null}, the item will be filtered
     * out.
     *
     * @param name               name of the sink
     * @param connectionSupplier MongoDB client supplier
     * @param <T>                type of the queried item
     */
    public static <T> MongoDBSourceBuilder<T> builder(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder<>(name, connectionSupplier);
    }

    /**
     * Convenience for {@link #builder(String, SupplierEx)} as a {@link
     * BatchSource}.
     */
    public static BatchSource<Document> mongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return MongoDBSources
                .<Document>builder(name, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(database))
                .collectionFn(db -> db.getCollection(collection))
                .destroyFn(MongoClient::close)
                .batch(col -> col.find().filter(filter).projection(projection), identity());
    }

    /**
     * Convenience for {@link #builder(String, SupplierEx)} as a {@link
     * StreamSource}.
     */
    public static StreamSource<Document> streamMongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return MongoDBSources
                .<Document>builder(name, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(database))
                .collectionFn(db -> db.getCollection(collection))
                .destroyFn(MongoClient::close)
                .stream(
                        col -> {
                            List<Bson> aggregates = new ArrayList<>();
                            if (filter != null) {
                                aggregates.add(Aggregates.match(filter));
                            }
                            if (projection != null) {
                                aggregates.add(Aggregates.project(projection));
                            }
                            ChangeStreamIterable<? extends Document> watch;
                            if (aggregates.isEmpty()) {
                                watch = col.watch();
                            } else {
                                watch = col.watch(aggregates);
                            }
                            return watch;
                        },
                        ChangeStreamDocument::getFullDocument
                );
    }
}
