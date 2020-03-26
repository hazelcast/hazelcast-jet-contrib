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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.contrib.cdc.ChangeEventElement;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import javax.annotation.Nonnull;
import javax.ws.rs.ProcessingException;
import java.util.Optional;
import java.util.function.Supplier;

class ChangeEventElementRelationalImpl implements ChangeEventElement {

    private final ThrowingSupplier<JsonNode, ParsingException> node;
    private final Supplier<String> json;

    private final ThrowingFunction<Class<?>, Object, ParsingException> object; //todo: rename to mapper

    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;

    ChangeEventElementRelationalImpl(@Nonnull String keyJson, @Nonnull ObjectMapper mapper) {
        this(new LazyThrowingSupplier<>(() -> toNode(keyJson, mapper)), () -> keyJson, mapper);
    }

    ChangeEventElementRelationalImpl(@Nonnull JsonNode keyNode, @Nonnull ObjectMapper mapper) {
        this(() -> keyNode, keyNode::textValue, mapper);
    }

    private ChangeEventElementRelationalImpl(
            @Nonnull ThrowingSupplier<JsonNode, ParsingException> node,
            @Nonnull Supplier<String> json,
            @Nonnull ObjectMapper mapper) {
        this.node = node;
        this.json = json;

        this.object = new LazyThrowingFunction<>((clazz) -> toObject(getNode(), clazz, mapper));

        this.integers = (key) -> parseIntegers(node.get(), key);
    }

    @Override
    public <T> T map(Class<T> clazz) throws ParsingException {
        Optional<T> o = (Optional<T>) object.apply(clazz);
        if (o.isPresent()) {
            return o.get();
        } else {
            throw new ParsingException("Failed mapping key into a " + clazz.getName());
        }
    }

    @Override
    public String asJson() {
        return json.get();
    }

    @Override
    public Optional<Object> getObject(String key) throws ParsingException  {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<String> getString(String key) throws ParsingException  {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return integers.apply(key);
    }

    @Override
    public Optional<Long> getLong(String key) throws ParsingException  {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Double> getDouble(String key) throws ParsingException  {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Boolean> getBoolean(String key) throws ParsingException  {
        return Optional.empty(); //todo
    }

    protected JsonNode getNode() throws ParsingException {
        return node.get();
    }

    @Override
    public String toString() {
        return asJson();
    }

    private static JsonNode toNode(String keyJson, ObjectMapper mapper) throws ProcessingException {
        try {
            return mapper.readTree(keyJson);
        } catch (Exception e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }

    private static Optional<Object> toObject(JsonNode node, Class<?> clazz, ObjectMapper mapper) throws ParsingException {
        try {
            Object value = mapper.treeToValue(node, clazz);
            return value == null ? Optional.empty() : Optional.of(value);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    private static Optional<Integer> parseIntegers(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return Optional.ofNullable(value == null ? null : value.intValue());
    }
}
