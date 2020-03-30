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

package com.hazelcast.jet.contrib.mqtt;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Contains methods to create MQTT sources.
 */
public final class MqttSources {

    private MqttSources() {
    }

    /**
     * Creates a {@link BatchSource} which reads strings from an input list.
     *
     * @param inputList list of strings
     * @return a batch source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(BatchSource)}
     */
    public static BatchSource<String> testListBatch(List<String> inputList) {

        return SourceBuilder
                .batch("list-source", x -> inputList)
                .<String>fillBufferFn((str, buf) -> {
                    for (String entry : inputList) {
                        buf.add(entry);
                    }
                    buf.close();
                })
                .build();

    }

    /**
     * Creates a {@link StreamSource} which reads strings from an input list.
     *
     * @param inputList list of strings
     * @return a stream source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<String> testListStream(List<String> inputList) {

        return SourceBuilder
                .stream("list-source-stream", x -> inputList)
                .<String>fillBufferFn((str, buf) -> {
                    for (String entry : inputList) {
                        buf.add(entry);
                    }
                })
                .build();

    }

    public static StreamSource<String> testUrlStream(String uri) {
        StreamSource<String> httpSource = SourceBuilder
                .stream("http-source", ctx -> HttpClients.createDefault())
                .<String>fillBufferFn((httpc, buf) -> {
                    HttpGet request = new HttpGet(uri);
                    InputStream content = httpc.execute(request).getEntity().getContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
                    reader.lines().forEach(buf::add);
                })
                .destroyFn(CloseableHttpClient::close)
                .build();
        return httpSource;
    }

}
