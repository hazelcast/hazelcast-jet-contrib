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
import com.hazelcast.jet.contrib.cdc.ChangeEventValue;
import com.hazelcast.jet.contrib.cdc.Operation;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazySupplier;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;
import org.bson.Document;

import java.util.Objects;
import java.util.function.Supplier;

public class ChangeEventValueMongoImpl implements ChangeEventValue {

    private final String json;
    private final Supplier<Long> timestamp;
    private final Supplier<Operation> operation;
    private final ThrowingSupplier<ChangeEventElement, ParsingException> before;
    private final ThrowingSupplier<ChangeEventElement, ParsingException> after;
    private final ThrowingSupplier<ChangeEventElement, ParsingException> patch;

    public ChangeEventValueMongoImpl(String valueJson) {
        Objects.requireNonNull(valueJson, "valueJson");

        Document document = Document.parse(valueJson);
        this.timestamp = new LazySupplier<>(() -> document.getLong("ts_ms"));
        this.operation = new LazySupplier<>(() -> Operation.get(document.getString("op")));
        this.before = new LazyThrowingSupplier<>(() -> new ChangeEventElementMongoImpl(subDocument(document, "before")));
        this.after = new LazyThrowingSupplier<>(() -> new ChangeEventElementMongoImpl(subDocument(document, "after")));
        this.patch = new LazyThrowingSupplier<>(() -> new ChangeEventElementMongoImpl(subDocument(document, "patch")));
        this.json = valueJson;
    }

    @Override
    public long timestamp() {
        return timestamp.get();
    }

    @Override
    public Operation getOperation() {
        return operation.get();
    }

    @Override
    public ChangeEventElement before() throws ParsingException {
        return before.get();
    }

    @Override
    public ChangeEventElement after() throws ParsingException {
        return after.get();
    }

    @Override
    public ChangeEventElement change() throws ParsingException {
        return patch.get();
    }

    @Override
    public String asJson() {
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

    private Document subDocument(Document parent, String key) throws ParsingException {
        try {
            String json = parent.getString(key);
            return json == null ? new Document() : Document.parse(json);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

}
