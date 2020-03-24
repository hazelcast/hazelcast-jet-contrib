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

import com.hazelcast.jet.contrib.cdc.ChangeEventValue;
import com.hazelcast.jet.contrib.cdc.Operation;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazySupplier;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;
import org.bson.Document;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventValueMongoImpl implements ChangeEventValue {

    private final String json;
    private final Supplier<Operation> operation;
    private final ThrowingSupplier<Optional<Document>, ParsingException> before;
    private final ThrowingSupplier<Optional<Document>, ParsingException> after;
    private final ThrowingSupplier<Optional<Document>, ParsingException> patch;

    public ChangeEventValueMongoImpl(String valueJson) {
        Objects.requireNonNull(valueJson, "valueJson");

        Document document = Document.parse(valueJson);
        this.operation = new LazySupplier<>(() -> Operation.get(document.getString("op")));
        this.before = new LazyThrowingSupplier<>(() -> subDocument(document, "before"));
        this.after = new LazyThrowingSupplier<>(() -> subDocument(document, "after"));
        this.patch = new LazyThrowingSupplier<>(() -> subDocument(document, "patch"));
        this.json = valueJson;
    }

    @Override
    public Operation getOperation() {
        return operation.get();
    }

    @Override
    public <T> T mapImage(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }

        Optional<Document> after = this.after.get();
        if (after.isPresent()) {
            return (T) after.get();
        }

        Optional<Document> before = this.before.get();
        if (before.isPresent()) {
            return (T) before.get();
        }

        throw new UnsupportedOperationException(ChangeEventValueMongoImpl.class.getSimpleName() +
                "for operation '" + operation + "' doesn't have either a 'before' or 'after' image");
    }

    @Override
    public <T> T mapUpdate(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }

        Optional<Document> patch = this.patch.get();
        if (patch.isPresent()) {
            return (T) patch.get();
        }

        throw new UnsupportedOperationException(ChangeEventValueMongoImpl.class.getSimpleName() +
                "for operation '" + operation + "' doesn't have an update patch");
    }

    @Override
    public String asJson() {
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

    private Optional<Document> subDocument(Document parent, String key) throws ParsingException {
        try {
            String json = parent.getString(key);
            return Optional.ofNullable(json == null ? null : Document.parse(json));
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

}
