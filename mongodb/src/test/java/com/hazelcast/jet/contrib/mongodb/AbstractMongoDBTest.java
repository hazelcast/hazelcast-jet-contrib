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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class AbstractMongoDBTest {

    static final String SOURCE_NAME = "source";
    static final String SINK_NAME = "sink";
    static final String DB_NAME = "db";
    static final String COL_NAME = "col";

    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer().withReplicaSetName("rs0");

    MongoClient mongo;
    JetInstance jet;

    @Before
    public void setUp() {
        mongoContainer.initializeReplicaSet();
        mongo = mongoContainer.newMongoClient();

        jet = Jet.newJetInstance();
    }

    public MongoCollection<Document> collection() {
        return mongo.getDatabase(DB_NAME).getCollection(COL_NAME);
    }

    @After
    public void tearDown() {
        jet.shutdown();
        mongo.close();
    }

}
