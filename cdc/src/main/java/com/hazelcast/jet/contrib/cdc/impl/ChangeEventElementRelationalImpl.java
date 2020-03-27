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
import java.util.Optional;
import java.util.function.Supplier;

class ChangeEventElementRelationalImpl implements ChangeEventElement {

    private final ThrowingSupplier<JsonNode, ParsingException> node;
    private final Supplier<String> json;

    private final ThrowingFunction<Class<?>, Object, ParsingException> mapper;

    private final ThrowingFunction<String, Optional<Object>, ParsingException> objects;
    private final ThrowingFunction<String, Optional<String>, ParsingException> strings;
    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;
    private final ThrowingFunction<String, Optional<Long>, ParsingException> longs;
    private final ThrowingFunction<String, Optional<Double>, ParsingException> doubles;
    private final ThrowingFunction<String, Optional<Boolean>, ParsingException> booleans;

    ChangeEventElementRelationalImpl(@Nonnull String json, @Nonnull ObjectMapper mapper) {
        this(new LazyThrowingSupplier<>(RelationalParsing.parse(json, mapper)), () -> json, mapper);
    }

    ChangeEventElementRelationalImpl(@Nonnull JsonNode node, @Nonnull ObjectMapper mapper) {
        this(() -> node, node::textValue, mapper);
    }

    private ChangeEventElementRelationalImpl(
            @Nonnull ThrowingSupplier<JsonNode, ParsingException> node,
            @Nonnull Supplier<String> json,
            @Nonnull ObjectMapper mapper) {
        this.node = node;
        this.json = json;

        this.mapper = new LazyThrowingFunction<>((clazz) -> RelationalParsing.map(node.get(), clazz, mapper));

        this.objects = (key) -> RelationalParsing.getObject(node.get(), key);
        this.strings = (key) -> RelationalParsing.getString(node.get(), key);
        this.integers = (key) -> RelationalParsing.getInteger(node.get(), key);
        this.longs = (key) -> RelationalParsing.getLong(node.get(), key);
        this.doubles = (key) -> RelationalParsing.getDouble(node.get(), key);
        this.booleans = (key) -> RelationalParsing.getBoolean(node.get(), key);
    }

    @Override
    public <T> T map(Class<T> clazz) throws ParsingException {
        return (T) mapper.apply(clazz);
    }

    @Override
    public Optional<Object> getObject(String key) throws ParsingException {
        return objects.apply(key);
    }

    @Override
    public Optional<String> getString(String key) throws ParsingException {
        return strings.apply(key);
    }

    @Override
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return integers.apply(key);
    }

    @Override
    public Optional<Long> getLong(String key) throws ParsingException {
        return longs.apply(key);
    }

    @Override
    public Optional<Double> getDouble(String key) throws ParsingException {
        return doubles.apply(key);
    }

    @Override
    public Optional<Boolean> getBoolean(String key) throws ParsingException {
        return booleans.apply(key);
    }

    @Override
    public String asJson() {
        return json.get();
    }

    @Override
    public String toString() {
        return asJson();
    }

}
