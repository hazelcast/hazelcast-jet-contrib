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
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

import static com.hazelcast.jet.elasticsearch.ElasticsearchSinks.buildClient;

/**
 * Contains factory methods for Elasticsearch sources
 */
public final class ElasticsearchSources {

    private static final String DEFAULT_SCROLL_TIMEOUT = "60s";

    private ElasticsearchSources() {
    }

    /**
     * Creates a source which queries objects from the specified elastic-search
     * using scrolling.
     *
     * @param name                  Name of the source
     * @param clientSupplier        Elasticsearch rest client supplier
     * @param searchRequestSupplier Search request supplier
     * @param scrollTimeout         scroll keep alive time
     * @param hitMapperFn           maps search hits to output items
     * @param optionsFn             obtains a {@link RequestOptions} for each request
     * @param destroyFn             called upon completion to release any resource
     * @param <T>                   type of items emitted downstream
     */
    public static <T> BatchSource<T> elasticSearch(String name,
                                                   SupplierEx<RestHighLevelClient> clientSupplier,
                                                   SupplierEx<SearchRequest> searchRequestSupplier,
                                                   String scrollTimeout,
                                                   FunctionEx<SearchHit, T> hitMapperFn,
                                                   FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
                                                   ConsumerEx<RestHighLevelClient> destroyFn

    ) {
        return SourceBuilder
                .batch(name, ctx -> new SearchContext<>(clientSupplier.get(), scrollTimeout,
                        hitMapperFn, searchRequestSupplier.get(), optionsFn, destroyFn))
                .<T>fillBufferFn(SearchContext::fillBuffer)
                .destroyFn(SearchContext::close)
                .build();
    }

    /**
     * Convenience for {@link #elasticSearch(String, SupplierEx, SupplierEx, String, FunctionEx, FunctionEx, ConsumerEx)}.
     * Uses {@link #DEFAULT_SCROLL_TIMEOUT} for scroll timeout and {@link
     * RequestOptions#DEFAULT}, emits string representation of items using
     * {@link SearchHit#getSourceAsString()} and closes the {@link
     * RestHighLevelClient} upon completion.
     */
    public static BatchSource<String> elasticSearch(String name,
                                                    SupplierEx<RestHighLevelClient> clientSupplier,
                                                    SupplierEx<SearchRequest> searchRequestSupplier
    ) {
        return elasticSearch(name, clientSupplier, searchRequestSupplier, DEFAULT_SCROLL_TIMEOUT,
                SearchHit::getSourceAsString, request -> RequestOptions.DEFAULT, RestHighLevelClient::close);
    }


    /**
     * Convenience for {@link #elasticSearch(String, SupplierEx, SupplierEx)}.
     * Rest client is configured with basic authentication.
     */
    public static BatchSource<String> elasticSearch(String name,
                                                    String username, String password,
                                                    String hostname, int port,
                                                    SupplierEx<SearchRequest> searchRequestSupplier
    ) {
        return elasticSearch(name, () -> buildClient(username, password, hostname, port),
                searchRequestSupplier);
    }

    private static final class SearchContext<T> {

        private final RestHighLevelClient client;
        private final String scrollInterval;
        private final FunctionEx<SearchHit, T> hitMapperFn;
        private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
        private final ConsumerEx<RestHighLevelClient> destroyFn;

        private SearchResponse searchResponse;

        private SearchContext(RestHighLevelClient client, String scrollInterval,
                              FunctionEx<SearchHit, T> hitMapperFn, SearchRequest searchRequest,
                              FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
                              ConsumerEx<RestHighLevelClient> destroyFn
        ) throws IOException {
            this.client = client;
            this.scrollInterval = scrollInterval;
            this.hitMapperFn = hitMapperFn;
            this.optionsFn = optionsFn;
            this.destroyFn = destroyFn;

            searchRequest.scroll(scrollInterval);
            searchResponse = client.search(searchRequest, optionsFn.apply(searchRequest));
        }

        private void fillBuffer(SourceBuffer<T> buffer) throws IOException {
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits == null || hits.length == 0) {
                buffer.close();
                return;
            }
            for (SearchHit hit : hits) {
                T item = hitMapperFn.apply(hit);
                if (item != null) {
                    buffer.add(item);
                }
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId());
            scrollRequest.scroll(scrollInterval);
            searchResponse = client.scroll(scrollRequest, optionsFn.apply(scrollRequest));
        }

        private void clearScroll() throws IOException {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        }

        private void close() throws IOException {
            clearScroll();
            destroyFn.accept(client);
        }
    }
}
