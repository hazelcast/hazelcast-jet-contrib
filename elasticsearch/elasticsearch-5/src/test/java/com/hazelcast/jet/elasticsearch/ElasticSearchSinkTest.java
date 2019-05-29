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

package com.hazelcast.jet.elasticsearch;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.elasticsearch.ElasticSearchSinks.elasticSearch;

public class ElasticSearchSinkTest extends ElasticSearchBaseTest {

    @Test
    public void test_elasticSearchSink() throws IOException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list(userList))
         .drainTo(elasticSearch(indexName, DEFAULT_USER, DEFAULT_PASS, container.getContainerIpAddress(),
                 mappedPort(), indexFn(indexName)));

        jet.newJob(p).join();

        assertIndexes();
    }
}
