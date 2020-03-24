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

package com.hazelcast.jet.contrib.cdc.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.contrib.cdc.ChangeEventValue;
import com.hazelcast.jet.contrib.cdc.Operation;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazySupplier;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventValueRelationalImpl implements ChangeEventValue {

    private final String json;
    private final Supplier<Operation> operation;
    private final ThrowingFunction<Class<?>, Object, ParsingException> before;
    private final ThrowingFunction<Class<?>, Object, ParsingException> after;

    public ChangeEventValueRelationalImpl(String valueJson, ObjectMapper mapper) throws ParsingException {
        Objects.requireNonNull(valueJson, "valueJson");
        Objects.requireNonNull(mapper, "mapper");

        Content content = parseContent(valueJson, mapper);
        this.operation = new LazySupplier<>(() -> Operation.get(content.operation));
        this.after = new LazyThrowingFunction<>((clazz) -> toObject(content.after, clazz, mapper));
        this.before = new LazyThrowingFunction<>((clazz) -> toObject(content.before, clazz, mapper));
        this.json = valueJson;
    }

    @Override
    public Operation getOperation() {
        return operation.get();
    }

    @Override
    public <T> T getImage(Class<T> clazz) throws ParsingException {
        Optional<T> after = (Optional<T>) this.after.apply(clazz);
        if (after.isPresent()) {
            return after.get();
        }

        Optional<T> before = (Optional<T>) this.before.apply(clazz);
        if (before.isPresent()) {
            return before.get();
        }

        throw new IllegalStateException(ChangeEventValueRelationalImpl.class.getSimpleName() +
                " should have either a 'before' or 'after' value");
    }

    @Override
    public <T> T getUpdate(Class<T> clazz) {
        throw new UnsupportedOperationException("Not supported for relational databases");
    }

    @Override
    public String asJson() {
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

    private static Optional<Object> toObject(JsonNode node, Class<?> clazz, ObjectMapper mapper) throws ParsingException {
        try {
            Object value = mapper.treeToValue(node, clazz);
            return value == null ? Optional.empty() : Optional.of(value);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    private static Content parseContent(String valueJson, ObjectMapper mapper) throws ParsingException {
        try {
            return mapper.readValue(valueJson, Content.class);
        } catch (Exception e) {
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
