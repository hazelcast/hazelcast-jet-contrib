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
import com.hazelcast.jet.contrib.cdc.FlatValues;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import java.util.Optional;

public class AbstractRelationalFlatValues implements FlatValues {

    private final ThrowingSupplier<JsonNode, ParsingException> node;

    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;

    public AbstractRelationalFlatValues(ThrowingSupplier<JsonNode, ParsingException> node) {
        this.node = node;

        this.integers = (key) -> parseIntegers(node.get(), key);
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

    private static Optional<Integer> parseIntegers(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return Optional.ofNullable(value == null ? null : value.intValue());
    }
}
