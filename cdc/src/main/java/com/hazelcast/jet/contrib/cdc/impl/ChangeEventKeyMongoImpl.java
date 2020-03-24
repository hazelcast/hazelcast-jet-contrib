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

import com.hazelcast.jet.contrib.cdc.ChangeEventKey;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingFunction;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;
import org.bson.Document;

import javax.ws.rs.ProcessingException;
import java.util.Objects;

public class ChangeEventKeyMongoImpl implements ChangeEventKey {

    private final String json;
    private final ThrowingSupplier<Document, ParsingException> document;
    private final ThrowingFunction<String, Integer, ParsingException> id;

    public ChangeEventKeyMongoImpl(String keyJson) {
        Objects.requireNonNull(keyJson, "keyJson");

        this.json = keyJson;
        this.document = new LazyThrowingSupplier<>(() -> toDocument(keyJson));
        this.id = new LazyThrowingFunction<>((idName) -> parseId(document.get(), idName));
    }

    @Override
    public <T> T get(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }
        return (T) document.get();
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

    private static Document toDocument(String keyJson) throws ParsingException {
        try {
            return Document.parse(keyJson);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    private static Integer parseId(Document document, String idName) throws ProcessingException {
        try {
            String stringId = document.getString(idName);
            return Integer.valueOf(stringId);
        } catch (Exception e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }
}
