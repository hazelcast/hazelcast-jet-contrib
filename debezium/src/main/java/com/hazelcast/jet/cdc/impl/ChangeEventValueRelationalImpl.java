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
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.util.LazySupplier;
import com.hazelcast.jet.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.cdc.util.ThrowingFunction;

import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventValueRelationalImpl implements ChangeEventValue {

    private final Supplier<Operation> operation;
    private final ThrowingFunction<Class<?>, Object, ParsingException> before;
    private final ThrowingFunction<Class<?>, Object, ParsingException> after;
    private final Supplier<String> printForm;

    public ChangeEventValueRelationalImpl(String valueJson, ObjectMapper objectMapper) throws ParsingException {
        this.printForm = () -> valueJson;

        Content content = parseContent(valueJson, objectMapper);
        this.operation = new LazySupplier<>(() -> Operation.get(content.operation));
        this.after = new LazyThrowingFunction<>((clazz) -> toObject(content.after, clazz, objectMapper));
        this.before = new LazyThrowingFunction<>((clazz) -> toObject(content.before, clazz, objectMapper));
    }

    @Override
    public Operation getOperation() {
        return operation.get();
    }

    @Override
    public <T> Optional<T> getBefore(Class<T> clazz) throws ParsingException {
        return (Optional<T>) before.apply(clazz);
    }

    @Override
    public <T> Optional<T> getAfter(Class<T> clazz) throws ParsingException {
        return (Optional<T>) after.apply(clazz);
    }

    @Override
    public <T> Optional<T> getCustom(String name, Class<T> clazz) throws ParsingException {
        throw new UnsupportedOperationException("Not supported for relational databases");
    }

    @Override
    public String toString() {
        return printForm.get();
    }

    private static Optional<Object> toObject(JsonNode node, Class<?> clazz, ObjectMapper objectMapper)
                                                                                        throws ParsingException {
        try {
            Object value = objectMapper.treeToValue(node, clazz);
            return value == null ? Optional.empty() : Optional.of(value);
        } catch (JsonProcessingException e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    private static Content parseContent(String valueJson, ObjectMapper objectMapper) throws ParsingException {
        try {
            return objectMapper.readValue(valueJson, Content.class);
        } catch (JsonProcessingException e) {
            throw new ParsingException(e.getMessage(), e);
        }
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
