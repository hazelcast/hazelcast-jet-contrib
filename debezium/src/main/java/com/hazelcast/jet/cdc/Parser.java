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

package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.cdc.impl.ChangeEventValueMongoImpl;
import com.hazelcast.jet.cdc.impl.ChangeEventValueRelationalImpl;

/**
 * TODO: javadoc
 */
public final class Parser {

    private final ObjectMapper objectMapper;

    /**
     * TODO: javadoc
     */
    public Parser() {
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * TODO: javadoc
     */
    public ChangeEventValue getChangeEventValue(String json) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(json);
        String source = getSource(jsonNode);
        if ("mongodb".equals(source)) {
            return new ChangeEventValueMongoImpl(jsonNode);
        } else {
            return new ChangeEventValueRelationalImpl(objectMapper, jsonNode);
        }
    }

    private static String getSource(JsonNode jsonNode) {
        JsonNode sourceNode = jsonNode.get("source");
        JsonNode connectorNode = sourceNode.get("connector");
        return connectorNode.textValue();
    }
}
