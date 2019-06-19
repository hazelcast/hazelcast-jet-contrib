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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MongoDBSourceTest extends AbstractMongoDBTest {

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
        p.drawFrom(MongoDBSources.mongodb(SOURCE_NAME, connectionString, DB_NAME, COL_NAME,
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
                MongoDBSources.streamMongodb(
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


}
