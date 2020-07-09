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

package com.hazelcast.jet.contrib.http;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.contrib.http.domain.User;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import org.junit.Test;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class HttpListenerSourceTest extends HttpTestBase {

    private static final int ITEM_COUNT = 100;
    private static final int FILTER_OUT_BELOW = 80;

    @Test
    public void testHttpIngestion_with_objectMapping() throws Throwable {
        int port = 5901;
        StreamSource<User> source = HttpListenerSources.httpListener(port, User.class);
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, false);
        postUsers(httpClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpIngestion_with_rawJsonString() throws Throwable {
        int port = HttpListenerBuilder.DEFAULT_PORT;
        StreamSource<String> source = HttpListenerSources.httpListener();
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, false);
        postUsers(httpClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpIngestion_with_customDeserializer() throws Throwable {
        int port = 5901;
        StreamSource<User> source = HttpListenerSources.httpListener(port,
                bytes -> JsonUtil.beanFrom(new String(bytes), User.class));
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, false);
        postUsers(httpClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    @Test
    public void testHttpsIngestion_with_objectMapping() throws Throwable {
        int port = HttpListenerBuilder.DEFAULT_PORT;
        StreamSource<User> source = HttpListenerSources.builder().sslContextFn(sslContextFn()).type(User.class).build();
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, true);
        postUsers(httpsClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }


    @Test
    public void testHttpsIngestion_with_rawJsonString() throws Throwable {
        int port = HttpListenerBuilder.DEFAULT_PORT;
        StreamSource<String> source = HttpListenerSources.builder().sslContextFn(sslContextFn()).build();
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, true);
        postUsers(httpsClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    @Test
    public void testHttpsIngestion_with_customDeserializer() throws Throwable {
        int port = HttpListenerBuilder.DEFAULT_PORT;
        StreamSource<User> source = HttpListenerSources
                .builder()
                .sslContextFn(sslContextFn())
                .mapToItemFn(bytes -> JsonUtil.beanFrom(new String(bytes), User.class))
                .build();
        Job job = startJob(source);

        String httpEndpoint = httpEndpointAddress(jet, port, true);
        postUsers(httpsClient, ITEM_COUNT, httpEndpoint);

        expectedException.expectCause(instanceOf(AssertionCompletedException.class));
        executeAndPeel(job);
    }

    private <T> Job startJob(StreamSource<T> source) {
        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withoutTimestamps()
         .map(item -> {
             if (item instanceof User) {
                 return (User) item;
             }
             return JsonUtil.beanFrom(item.toString(), User.class);
         })
         .filter(user -> user.getId() >= FILTER_OUT_BELOW)
         .writeTo(assertCollectedEventually(30,
                 list -> assertEquals(ITEM_COUNT - FILTER_OUT_BELOW, list.size())));

        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        return job;
    }

}
