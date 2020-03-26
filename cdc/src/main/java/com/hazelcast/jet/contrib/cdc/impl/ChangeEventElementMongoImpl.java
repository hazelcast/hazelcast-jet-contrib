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

import com.hazelcast.jet.contrib.cdc.ChangeEventElement;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;
import org.bson.Document;

import javax.ws.rs.ProcessingException;
import java.util.Optional;
import java.util.function.Supplier;

class ChangeEventElementMongoImpl implements ChangeEventElement {

    private final ThrowingSupplier<Document, ParsingException> document;
    private final Supplier<String> json;

    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;

    ChangeEventElementMongoImpl(String keyJson) {
        this(new LazyThrowingSupplier<>(() -> toDocument(keyJson)), () -> keyJson);
    }

    ChangeEventElementMongoImpl(Document document) {
        this(() -> document, document::toJson);
    }

    private ChangeEventElementMongoImpl(ThrowingSupplier<Document, ParsingException> document, Supplier<String> json) {
        this.document = document;
        this.json = json;

        this.integers = (key) -> parseInteger(document.get(), key);
    }

    @Override
    public <T> T map(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }
        return (T) getDocument();
    }

    @Override
    public String asJson() {
        return json.get();
    }

    @Override
    public Optional<Object> getObject(String key) throws ParsingException {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<String> getString(String key) throws ParsingException {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return integers.apply(key);
    }

    @Override
    public Optional<Long> getLong(String key) throws ParsingException {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Double> getDouble(String key) throws ParsingException {
        return Optional.empty(); //todo
    }

    @Override
    public Optional<Boolean> getBoolean(String key) throws ParsingException {
        return Optional.empty(); //todo
    }

    public Document getDocument() throws ParsingException {
        return document.get();
    }

    @Override
    public String toString() {
        return asJson();
    }

    private static Document toDocument(String keyJson) throws ParsingException {
        try {
            return Document.parse(keyJson);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    private static Optional<Integer> parseInteger(Document document, String key) throws ProcessingException {
        Object object = document.get(key);
        if (object instanceof Number) {
            return Optional.of(((Number) object).intValue());
        } else if (object instanceof String) {
            try {
                return Optional.of(new Integer((String) object));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }
}
