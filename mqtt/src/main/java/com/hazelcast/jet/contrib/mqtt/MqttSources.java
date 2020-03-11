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

import java.util.List;

/**
 * Contains methods to create MQTT sources.
 */
public class MqttSources {

    public static BatchSource<String> testList(List<String> inputList) {

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

}
