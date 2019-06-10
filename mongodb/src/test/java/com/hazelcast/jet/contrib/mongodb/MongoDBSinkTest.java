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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongoDBSinkTest extends AbstractMongoDBTest {

    @Test
    public void test() {
        IListJet<Integer> list = jet.getList("list");
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }

        String connectionString = mongoContainer.connectionString();

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list(list))
         .map(i -> new Document("key", i))
         .drainTo(MongoDBSinks.mongodb(SINK_NAME, connectionString, DB_NAME, COL_NAME));

        jet.newJob(p).join();

        MongoCollection<Document> collection = collection();
        assertEquals(100, collection.countDocuments());
        assertEquals(0, collection.find().first().get("key"));
    }


}
