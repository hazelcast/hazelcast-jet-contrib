/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.contrib.cdc;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.mongodb.MongoDBContainer;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;

public class MongoDbIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public MongoDBContainer mongo = new MongoDBContainer("debezium/example-mongodb")
            .withCopyFileToContainer(MountableFile.forClasspathResource("alterMongoData.sh", 777),
                    "/usr/local/bin/alterData.sh");

    @Test
    public void customers() throws Exception {
        // populate initial data
        mongo.execInContainer("sh", "-c", "/usr/local/bin/init-inventory.sh");

        JetInstance jet = createJetMember();

        Pipeline pipeline = Pipeline.create();
        String[] expectedEvents = {
                "1001/0:SYNC:Document{{_id=1001, first_name=Sally, last_name=Thomas, email=sally.thomas@acme.com}}",
                "1002/0:SYNC:Document{{_id=1002, first_name=George, last_name=Bailey, email=gbailey@foobar.com}}",
                "1003/0:SYNC:Document{{_id=1003, first_name=Edward, last_name=Walker, email=ed@walker.com}}",
                "1004/0:SYNC:Document{{_id=1004, first_name=Anne, last_name=Kretchmar, email=annek@noanswer.org}}",
                "1004/1:UPDATE:Document{{_id=1004, first_name=Anne Marie, last_name=Kretchmar, email=annek@noanswer.org}}",
                "1005/0:INSERT:Document{{_id=1005, first_name=Jason, last_name=Bourne, email=jason@bourne.org}}",
                "1005/1:DELETE:Document{{}}"
        };
        pipeline.readFrom(CdcSources.mongodb("customers", connectorProperties("customers")))
                .withNativeTimestamps(0)
                .<ChangeEvent>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(event -> event.key().id("id"))
                .mapStateful(
                        State::new,
                        (state, customerId, event) -> {
                            ChangeEventValue eventValue = event.value();
                            Operation operation = eventValue.getOperation();
                            switch (operation) {
                                case SYNC:
                                case INSERT:
                                    state.set(eventValue.mapImage(Document.class));
                                    break;
                                case UPDATE:
                                    state.update(eventValue.mapUpdate(Document.class));
                                    break;
                                case DELETE:
                                    state.clear();
                                    break;
                                default:
                                    throw new UnsupportedOperationException(operation.name());
                            }
                            return customerId + "/" + state.updateCount() + ":" + operation + ":" + state.get();
                        })
                .setLocalParallelism(1)
                .writeTo(AssertionSinks.assertCollectedEventually(30, assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                .getClassLoader()
                .getResource("debezium-connector-mongodb.zip")));

        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        sleepAtLeastSeconds(10);
        // update record
        mongo.execInContainer("sh", "-c", "/usr/local/bin/alterData.sh");

        try {
            job.join();
            Assert.fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            Assert.assertTrue("Job was expected to complete with AssertionCompletedException, " +
                    "but completed with: " + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }

    }

    @Test
    public void orders() throws Exception {
        // populate initial data
        mongo.execInContainer("sh", "-c", "/usr/local/bin/init-inventory.sh");

        JetInstance jet = createJetMember();

        Pipeline pipeline = Pipeline.create();
        String[] expectedEvents = {
                "10001/0:SYNC:Document{{_id=10001, order_date=" + new Date(1452902400000L) +
                        ", purchaser_id=1001, quantity=1, product_id=102}}",
                "10002/0:SYNC:Document{{_id=10002, order_date=" + new Date(1452988800000L) +
                        ", purchaser_id=1002, quantity=2, product_id=105}}",
                "10003/0:SYNC:Document{{_id=10003, order_date=" + new Date(1455840000000L) +
                        ", purchaser_id=1002, quantity=2, product_id=106}}",
                "10004/0:SYNC:Document{{_id=10004, order_date=" + new Date(1456012800000L) +
                        ", purchaser_id=1003, quantity=1, product_id=107}}",
        };
        pipeline.readFrom(CdcSources.mongodb("orders", connectorProperties("orders")))
                .withoutTimestamps()
                .groupingKey(event -> getOrderNumber(event, "id"))
                .mapStateful(
                        State::new,
                        (state, orderId, event) -> {
                            ChangeEventValue eventValue = event.value();
                            Operation operation = eventValue.getOperation();
                            switch (operation) {
                                case SYNC:
                                case INSERT:
                                    state.set(eventValue.mapImage(Document.class));
                                    break;
                                case UPDATE:
                                    state.update(eventValue.mapUpdate(Document.class));
                                    break;
                                case DELETE:
                                    state.clear();
                                    break;
                                default:
                                    throw new UnsupportedOperationException(operation.name());
                            }
                            return orderId + "/" + state.updateCount() + ":" + operation + ":" + state.get();
                        })
                .setLocalParallelism(1)
                .writeTo(AssertionSinks.assertCollectedEventually(30, assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                .getClassLoader()
                .getResource("debezium-connector-mongodb.zip")));

        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        try {
            job.join();
            Assert.fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            Assert.assertTrue("Job was expected to complete with AssertionCompletedException, " +
                    "but completed with: " + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }

    }

    @Nonnull
    private Properties connectorProperties(String collectionName) {
        Properties properties = new Properties();
        properties.put("mongodb.hosts", "rs0/" + mongo.getContainerIpAddress() + ":"
                + mongo.getMappedPort(MongoDBContainer.MONGODB_PORT));
        properties.put("mongodb.name", "fullfillment");
        properties.put("mongodb.user", "debezium");
        properties.put("mongodb.password", "dbz");
        properties.put("mongodb.members.auto.discover", "false");
        properties.put("collection.whitelist", "inventory." + collectionName);
        return properties;
    }

    private static int getOrderNumber(ChangeEvent event, String idName) throws ParsingException {
        //pick random method for extracting ID in order to test all code paths
        boolean primitive = ThreadLocalRandom.current().nextBoolean();
        if (primitive) {
            return event.key().id(idName);
        } else {
            Document document = event.key().map(Document.class);
            return Integer.parseInt(document.getString(idName));
        }
    }

    private static class State {

        private int updates = -1;

        @Nonnull
        private Document document = new Document();

        Document get() {
            return document;
        }

        int updateCount() {
            return updates;
        }

        void set(@Nonnull Document document) {
            this.document = Objects.requireNonNull(document);
            updates++;
        }

        void update(@Nonnull Document document) {
            this.document.putAll((Document) document.get("$set")); //todo: blasphemy!
            updates++;
        }

        void clear() {
            document = new Document();
            updates++;
        }
    }
}
