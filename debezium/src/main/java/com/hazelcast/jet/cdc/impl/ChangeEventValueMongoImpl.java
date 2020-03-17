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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import org.bson.Document;

public class ChangeEventValueMongoImpl implements ChangeEventValue {

    private final JsonNode jsonNode;

    private Operation operation;
    private Object before;
    private Object after;

    public ChangeEventValueMongoImpl(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    @Override
    public Operation getOperation() {
        if (operation == null) {
            operation = Operation.get(jsonNode.get("op").textValue());
        }
        return operation;
    }

    @Override
    public <T> T getBefore(Class<T> clazz) throws JsonProcessingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as `org.bson.Document`");
            //todo: use POJO from BSON instead of Document
        }
        if (before == null) {
            before = Document.parse(jsonNode.get("before").asText());
        }
        return (T) before;
    }

    @Override
    public <T> T getAfter(Class<T> clazz) throws JsonProcessingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as `org.bson.Document`");
            //todo: use POJO from BSON instead of Document
        }
        if (after == null) {
            after = Document.parse(jsonNode.get("after").asText());
        }
        return (T) after;
    }

    //todo: deal with the "patch" field in update events
}
