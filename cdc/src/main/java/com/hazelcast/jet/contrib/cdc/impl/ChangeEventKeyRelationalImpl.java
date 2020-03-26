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

import javax.annotation.Nonnull;
import javax.ws.rs.ProcessingException;
import java.util.Optional;

public class ChangeEventKeyRelationalImpl extends AbstractRelationalFlatValues implements ChangeEventKey {

    private final String json;
    private final ThrowingFunction<Class<?>, Object, ParsingException> object;

    public ChangeEventKeyRelationalImpl(@Nonnull String keyJson, @Nonnull ObjectMapper mapper) {
        super(new LazyThrowingSupplier<>(() -> toNode(keyJson, mapper)));
        this.json = keyJson;
        this.object = new LazyThrowingFunction<>((clazz) -> toObject(getNode(), clazz, mapper));
    }

    public ChangeEventKeyRelationalImpl(@Nonnull JsonNode keyNode, @Nonnull ObjectMapper mapper) {
        super(() -> keyNode);
        this.json = keyNode.textValue();
        this.object = new LazyThrowingFunction<>((clazz) -> toObject(keyNode, clazz, mapper));
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
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

}
