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
import com.hazelcast.jet.contrib.cdc.ChangeEventKey;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import javax.annotation.Nonnull;
import javax.ws.rs.ProcessingException;
import java.util.Objects;
import java.util.Optional;

public class ChangeEventKeyRelationalImpl implements ChangeEventKey {

    private final String json;
    private final ThrowingSupplier<JsonNode, ParsingException> node;
    private final ThrowingFunction<String, Integer, ParsingException> id;
    private final ThrowingFunction<Class<?>, Object, ParsingException> object;

    public ChangeEventKeyRelationalImpl(@Nonnull String keyJson, @Nonnull ObjectMapper mapper) {
        Objects.requireNonNull(keyJson);
        Objects.requireNonNull(mapper);

        this.json = keyJson;
        this.node = new LazyThrowingSupplier<>(() -> toNode(keyJson, mapper));
        this.id = new LazyThrowingFunction<>((idName) -> parseId(node.get(), idName));
        this.object = new LazyThrowingFunction<>((clazz) -> toObject(node.get(), clazz, mapper));
    }

    @Override
    public <T> T get(Class<T> clazz) throws ParsingException {
        Optional<T> o = (Optional<T>) object.apply(clazz);
        if (o.isPresent()) {
            return o.get();
        } else {
            throw new ParsingException("Failed mapping key into a " + clazz.getName());
        }
    }

    @Override
    public int id(String idName) throws ParsingException {
        return id.apply(idName);
    }

    @Override
    public String asJson() {
        return json;
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

    private static Integer parseId(JsonNode keyNode, String idName) throws ProcessingException {
        try {
            return keyNode.get(idName).asInt();
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

}
