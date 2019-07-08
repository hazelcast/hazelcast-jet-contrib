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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDBSourceTest extends AbstractMongoDBTest {


    @Test
    public void testBatch_whenServerNotAvailable() {
        String connectionString = mongoContainer.connectionString();
        mongoContainer.close();

        IListJet<Document> list = jet.getList("list");

        BatchSource<Document> source = MongoDBSources
                .<Document>builder(SOURCE_NAME, () -> mongoClient(connectionString, 3))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .collectionFn(db -> db.getCollection(COL_NAME))
                .destroyFn(MongoClient::close)
                .batch(MongoCollection::find, doc -> doc);

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .drainTo(Sinks.list(list));

        try {
            jet.newJob(p).join();
            fail();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof JetException);
        }
    }

    @Test
    public void testStream_whenServerNotAvailable() {
        String connectionString = mongoContainer.connectionString();
        mongoContainer.close();

        IListJet<Document> list = jet.getList("list");

        StreamSource<? extends Document> source = MongoDBSources
                .<Document>builder(SOURCE_NAME, () -> mongoClient(connectionString, 3))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .collectionFn(db -> db.getCollection(COL_NAME))
                .destroyFn(MongoClient::close)
                .stream(MongoCollection::watch, ChangeStreamDocument::getFullDocument);

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .withNativeTimestamps(0)
         .drainTo(Sinks.list(list));

        try {
            jet.newJob(p).join();
            fail();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof JetException);
        }
    }

    @Test
    public void testBatch() {

        IListJet<Document> list = jet.getList("list");

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("key", i).append("val", i));
        }
        collection().insertMany(documents);


        String connectionString = mongoContainer.connectionString();

        Pipeline p = Pipeline.create();
        p.drawFrom(MongoDBSources.batch(SOURCE_NAME, connectionString, DB_NAME, COL_NAME,
                new Document("val", new Document("$gte", 10)),
                new Document("val", 1).append("_id", 0)))
         .drainTo(Sinks.list(list));

        jet.newJob(p).join();

        assertEquals(90, list.size());
        Document actual = list.get(0);
        assertNull(actual.get("key"));
        assertNull(actual.get("_id"));
        assertNotNull(actual.get("val"));
    }

    @Test
    public void testStream() {
        IListJet<Document> list = jet.getList("list");

        String connectionString = mongoContainer.connectionString();

        Pipeline p = Pipeline.create();
        p.drawFrom(
                MongoDBSources.stream(
                        SOURCE_NAME,
                        connectionString,
                        DB_NAME,
                        COL_NAME,
                        new Document("fullDocument.val", new Document("$gte", 10))
                                .append("operationType", "insert"),
                        new Document("fullDocument.val", 1).append("_id", 1)
                )
        )
         .withNativeTimestamps(0)
         .drainTo(Sinks.list(list));

        Job job = jet.newJob(p);


        collection().insertOne(new Document("val", 1));
        collection().insertOne(new Document("val", 10).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(1, list.size());
            Document document = list.get(0);
            assertEquals(10, document.get("val"));
            assertNull(document.get("foo"));
        });

        collection().insertOne(new Document("val", 2));
        collection().insertOne(new Document("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(2, list.size());
            Document document = list.get(1);
            assertEquals(20, document.get("val"));
            assertNull(document.get("foo"));
        });

        job.cancel();

    }

    @Test
    public void testStream_whenWatchDatabase() {
        IListJet<Document> list = jet.getList("list");

        String connectionString = mongoContainer.connectionString();

        StreamSource<Document> source = MongoDBSources
                .builder(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(DB_NAME))
                .destroyFn(MongoClient::close)
                .streamDatabase(db -> {
                            List<Bson> aggregates = new ArrayList<>();
                            aggregates.add(Aggregates.match(new Document("fullDocument.val", new Document("$gte", 10))
                                    .append("operationType", "insert")));

                            aggregates.add(Aggregates.project(new Document("fullDocument.val", 1).append("_id", 1)));
                            return db.watch(aggregates);
                        },
                        csd -> (Document) csd.getFullDocument()
                );

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .withNativeTimestamps(0)
         .drainTo(Sinks.list(list));

        Job job = jet.newJob(p);

        MongoCollection<Document> col1 = collection("col1");
        MongoCollection<Document> col2 = collection("col2");

        col1.insertOne(new Document("val", 1));
        col1.insertOne(new Document("val", 10).append("foo", "bar"));

        col2.insertOne(new Document("val", 1));
        col2.insertOne(new Document("val", 10).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(2, list.size());
            list.forEach(document -> {
                assertEquals(10, document.get("val"));
                assertNull(document.get("foo"));
            });
        });

        col1.insertOne(new Document("val", 2));
        col1.insertOne(new Document("val", 20).append("foo", "bar"));

        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(4, list.size());
            list.stream().skip(2).forEach(document -> {
                assertEquals(20, document.get("val"));
                assertNull(document.get("foo"));
            });
        });

        job.cancel();

    }

    @Test
    public void testStream_whenWatchAll() {
        IListJet<Document> list = jet.getList("list");

        String connectionString = mongoContainer.connectionString();

        StreamSource<Document> source = MongoDBSources
                .builder(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .destroyFn(MongoClient::close)
                .streamAll(client -> {
                            List<Bson> aggregates = new ArrayList<>();
                            aggregates.add(Aggregates.match(new Document("fullDocument.val", new Document("$gte", 10))
                                    .append("operationType", "insert")));

                            aggregates.add(Aggregates.project(new Document("fullDocument.val", 1).append("_id", 1)));
                            return client.watch(aggregates);
                        },
                        csd -> (Document) csd.getFullDocument()
                );

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .withNativeTimestamps(0)
         .drainTo(Sinks.list(list));

        Job job = jet.newJob(p);

        MongoCollection<Document> col1 = collection("db1", "col1");
        MongoCollection<Document> col2 = collection("db1", "col2");
        MongoCollection<Document> col3 = collection("db2", "col3");

        col1.insertOne(new Document("val", 1));
        col1.insertOne(new Document("val", 10).append("foo", "bar"));
        col2.insertOne(new Document("val", 1));
        col2.insertOne(new Document("val", 10).append("foo", "bar"));
        col3.insertOne(new Document("val", 1));
        col3.insertOne(new Document("val", 10).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(3, list.size());
            list.forEach(document -> {
                assertEquals(10, document.get("val"));
                assertNull(document.get("foo"));
            });
        });

        col1.insertOne(new Document("val", 2));
        col1.insertOne(new Document("val", 20).append("foo", "bar"));
        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 20).append("foo", "bar"));
        col2.insertOne(new Document("val", 2));
        col2.insertOne(new Document("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> {
            assertEquals(6, list.size());
            list.stream().skip(3).forEach(document -> {
                assertEquals(20, document.get("val"));
                assertNull(document.get("foo"));
            });
        });

        job.cancel();

    }

    static MongoClient mongoClient(String connectionString, int connectionTimeoutSeconds) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> {
                    b.serverSelectionTimeout(connectionTimeoutSeconds, SECONDS);
                })
                .build();

        return MongoClients.create(settings);
    }


}
