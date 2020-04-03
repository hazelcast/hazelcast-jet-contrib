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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.cdc.ChangeEventElement;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingBiFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

class ChangeEventElementJsonImpl implements ChangeEventElement {

    private final ThrowingSupplier<JsonNode, ParsingException> node;
    private final SupplierEx<String> json;

    private final ThrowingFunction<Class<?>, Object, ParsingException> mapper;

    private final ThrowingFunction<String, Optional<Object>, ParsingException> objects;
    private final ThrowingFunction<String, Optional<String>, ParsingException> strings;
    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;
    private final ThrowingFunction<String, Optional<Long>, ParsingException> longs;
    private final ThrowingFunction<String, Optional<Double>, ParsingException> doubles;
    private final ThrowingFunction<String, Optional<Boolean>, ParsingException> booleans;
    private final ThrowingBiFunction<String, Class<Object>, Optional<List<Optional<Object>>>, ParsingException> lists;

    ChangeEventElementJsonImpl(@Nonnull String json, @Nonnull ObjectMapper mapper) {
        this(new LazyThrowingSupplier<>(JsonParsing.parse(json, mapper)), () -> json, mapper);
    }

    ChangeEventElementJsonImpl(@Nonnull JsonNode node, @Nonnull ObjectMapper mapper) {
        this(() -> node, node::textValue, mapper);
    }

    private ChangeEventElementJsonImpl(
            @Nonnull ThrowingSupplier<JsonNode, ParsingException> node,
            @Nonnull SupplierEx<String> json,
            @Nonnull ObjectMapper mapper) {
        this.node = node;
        this.json = json;

        this.mapper = new LazyThrowingFunction<>((clazz) -> JsonParsing.mapToObj(node.get(), clazz, mapper));

        this.objects = (key) -> JsonParsing.getObject(node.get(), key);
        this.strings = (key) -> JsonParsing.getString(node.get(), key);
        this.integers = (key) -> JsonParsing.getInteger(node.get(), key);
        this.longs = (key) -> JsonParsing.getLong(node.get(), key);
        this.doubles = (key) -> JsonParsing.getDouble(node.get(), key);
        this.booleans = (key) -> JsonParsing.getBoolean(node.get(), key);
        this.lists = (key, clazz) -> JsonParsing.getList(node.get(), key, clazz);
    }

    @Override
    public <T> T mapToObj(Class<T> clazz) throws ParsingException {
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
    public <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException {
        return lists.apply(key, (Class) clazz);
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
