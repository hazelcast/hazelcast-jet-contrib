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

package com.hazelcast.jet.contrib.debezium;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.Parser;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.mongodb.MongoDBContainer;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import io.debezium.config.Configuration;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.utility.MountableFile;

import java.util.Objects;
import java.util.concurrent.CompletionException;

public class MongoDbIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public MongoDBContainer mongo = new MongoDBContainer("debezium/example-mongodb")
            .withCopyFileToContainer(MountableFile.forClasspathResource("alterMongoData.sh", 777),
                    "/usr/local/bin/alterData.sh");

    @Test
    public void readFromMongoDb() throws Exception {
        // populate initial data
        mongo.execInContainer("sh", "-c", "/usr/local/bin/init-inventory.sh");

        Configuration configuration = Configuration
                .create()
                .with("name", "mongodb-inventory-connector")
                .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                /* begin connector properties */
                .with("mongodb.hosts", "rs0/" + mongo.getContainerIpAddress() + ":"
                        + mongo.getMappedPort(MongoDBContainer.MONGODB_PORT))
                .with("mongodb.name", "fullfillment")
                .with("mongodb.user", "debezium")
                .with("mongodb.password", "dbz")
                .with("mongodb.members.auto.discover", "false")
                .with("collection.whitelist", "inventory.customers")
                .with("database.history.hazelcast.list.name", "test")
                .build();

        JetInstance jet = createJetMember();

        Pipeline pipeline = Pipeline.create();
        String[] expectedEvents = {
                "1001/0:SYNC:Document{{_id=1001, first_name=Sally, last_name=Thomas, email=sally.thomas@acme.com}}",
                "1002/0:SYNC:Document{{_id=1002, first_name=George, last_name=Bailey, email=gbailey@foobar.com}}",
                "1003/0:SYNC:Document{{_id=1003, first_name=Edward, last_name=Walker, email=ed@walker.com}}",
                "1004/0:SYNC:Document{{_id=1004, first_name=Anne, last_name=Kretchmar, email=annek@noanswer.org}}",
                "1005/0:SYNC:Document{{_id=1005, first_name=Jason, last_name=Bourne, email=jason@bourne.org}}"
        };
        pipeline.readFrom(DebeziumSources.cdc(configuration))
                .withoutTimestamps()
                .mapUsingService(ServiceFactories.nonSharedService(context -> new Parser()), Parser::getChangeEventValue)
                .groupingKey(changeEventValue -> changeEventValue.getAfter(Document.class).getLong("_id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, changeEventValue) -> {
                            long count = accumulator.get();
                            Operation operation = changeEventValue.getOperation();
                            Document customer = changeEventValue.getAfter(Document.class);
                            accumulator.add(1);
                            return customerId + "/" + count + ":" + operation + ":" + customer;
                        })
                .writeTo(AssertionSinks.assertCollectedEventually(30,
                        assertListFn(expectedEvents)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                .getClassLoader()
                .getResource("debezium-connector-mongodb.zip")));

        Job job = jet.newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

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
}
