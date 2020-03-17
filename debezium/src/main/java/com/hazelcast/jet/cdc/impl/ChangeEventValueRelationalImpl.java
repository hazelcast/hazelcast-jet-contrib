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

package com.hazelcast.jet.cdc.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;

public class ChangeEventValueRelationalImpl implements ChangeEventValue {

    private final ObjectMapper objectMapper;
    private final Content content;

    private Operation operation;
    private Object before;
    private Object after;

    public ChangeEventValueRelationalImpl(ObjectMapper objectMapper, JsonNode jsonNode) throws JsonProcessingException {
        this.objectMapper = objectMapper;
        this.content = objectMapper.treeToValue(jsonNode, Content.class);
    }

    @Override
    public Operation getOperation() {
        if (operation == null) {
            operation = Operation.get(content.operation);
        }
        return operation;
    }

    @Override
    public <T> T getBefore(Class<T> clazz) throws JsonProcessingException {
        if (before == null) {
            before = objectMapper.treeToValue(content.before, clazz);
        }
        return (T) before;
    }

    @Override
    public <T> T getAfter(Class<T> clazz) throws JsonProcessingException {
        if (after == null) {
            after = objectMapper.treeToValue(content.after, clazz);
        }
        return (T) after;
    }

    private static class Content {

        @JsonProperty("source")
        public JsonNode source;

        @JsonProperty("after")
        public JsonNode after;

        @JsonProperty("before")
        public JsonNode before;

        @JsonProperty("ts_ms")
        public long timestamp;

        @JsonProperty("op")
        public String operation;

    }
}
