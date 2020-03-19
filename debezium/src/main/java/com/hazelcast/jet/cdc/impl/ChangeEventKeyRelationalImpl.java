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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.cdc.ChangeEventKey;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.util.ThrowingSupplier;

import javax.ws.rs.ProcessingException;
import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventKeyRelationalImpl implements ChangeEventKey {

    private final ThrowingSupplier<Integer, ParsingException> id;
    private final Supplier<String> printForm;

    public ChangeEventKeyRelationalImpl(String keyJson, ObjectMapper objectMapper) {
        this.id = new LazyThrowingSupplier<>(() -> parseId(keyJson, objectMapper));
        this.printForm = () -> keyJson;
    }

    @Override
    public int id() throws ParsingException {
        return id.get().get();
    }

    @Override
    public String toString() {
        return printForm.get();
    }

    private Optional<Integer> parseId(String keyJson, ObjectMapper objectMapper) throws ProcessingException {
        try {
            return Optional.of(objectMapper.readTree(keyJson).get("id").asInt());
        } catch (JsonProcessingException e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }

}
