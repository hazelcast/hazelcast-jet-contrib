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

import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

import static org.apache.http.auth.AuthScope.ANY;

/**
 * Contains factory methods for ElasticSearch sinks
 */
public final class ElasticSearchSinks {

    private ElasticSearchSinks() {
    }

    /**
     * Creates a sink which indexes objects into the specified elastic-search
     * using bulk requests.
     *
     * @param name                Name of the created sink
     * @param clientSupplier      ElasticSearch rest client supplier
     * @param bulkRequestSupplier Bulk request supplier, will be called to obtain a
     *                            new {@link BulkRequest} instance after each call.
     * @param indexFn             Creates an {@link IndexRequest} for each object
     * @param destroyFn           called upon completion to release any resource
     */
    public static <T> Sink<T> elasticSearch(String name,
                                            SupplierEx<RestHighLevelClient> clientSupplier,
                                            SupplierEx<BulkRequest> bulkRequestSupplier,
                                            FunctionEx<T, IndexRequest> indexFn,
                                            ConsumerEx<RestHighLevelClient> destroyFn
    ) {
        return SinkBuilder
                .sinkBuilder("elasticSearch-" + name,
                        ctx -> new BulkContext(clientSupplier.get(), bulkRequestSupplier, destroyFn))
                .<T>receiveFn((bulkContext, item) -> bulkContext.add(indexFn.apply(item)))
                .flushFn(BulkContext::bulk)
                .destroyFn(BulkContext::close)
                .build();
    }

    /**
     * Convenience for {@link #elasticSearch(String, SupplierEx, SupplierEx, FunctionEx, ConsumerEx)}
     * Creates a new {@link BulkRequest} with default options for each batch and
     * closes the {@link RestHighLevelClient} upon completion.
     */
    public static <T> Sink<T> elasticSearch(String name,
                                            SupplierEx<RestHighLevelClient> clientSupplier,
                                            FunctionEx<T, IndexRequest> indexFn
    ) {
        return elasticSearch(name, clientSupplier, BulkRequest::new, indexFn, RestHighLevelClient::close);
    }

    /**
     * Convenience for {@link #elasticSearch(String, SupplierEx, SupplierEx, FunctionEx, ConsumerEx)}
     * Rest client is configured with basic authentication.
     */
    public static <T> Sink<T> elasticSearch(String name,
                                            String username, String password,
                                            String hostname, int port,
                                            FunctionEx<T, IndexRequest> indexFn
    ) {
        return elasticSearch(name, () -> buildClient(username, password, hostname, port), indexFn);
    }

    static RestHighLevelClient buildClient(String username, String password, String hostname, int port) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(ANY, new UsernamePasswordCredentials(username, password));
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port))
                          .setHttpClientConfigCallback(httpClientBuilder ->
                                  httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
        );
    }

    private static final class BulkContext {

        private final RestHighLevelClient client;
        private final SupplierEx<BulkRequest> bulkRequestSupplier;
        private final ConsumerEx<RestHighLevelClient> destroyFn;

        private BulkRequest bulkRequest;

        private BulkContext(RestHighLevelClient client, SupplierEx<BulkRequest> bulkRequestSupplier,
                            ConsumerEx<RestHighLevelClient> destroyFn) {
            this.client = client;
            this.bulkRequestSupplier = bulkRequestSupplier;
            this.destroyFn = destroyFn;

            this.bulkRequest = bulkRequestSupplier.get();
        }

        private void add(IndexRequest request) {
            bulkRequest.add(request);
        }

        private void bulk() throws IOException {
            client.bulk(bulkRequest);
            bulkRequest = bulkRequestSupplier.get();
        }

        private void close() throws IOException {
            destroyFn.accept(client);
        }
    }

}
