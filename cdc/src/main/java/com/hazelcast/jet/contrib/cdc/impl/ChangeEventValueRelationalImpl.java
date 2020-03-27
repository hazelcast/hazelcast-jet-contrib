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
import com.hazelcast.jet.contrib.cdc.ChangeEventValue;
import com.hazelcast.jet.contrib.cdc.Operation;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import java.util.Objects;
import java.util.Optional;

import static com.hazelcast.jet.contrib.cdc.impl.RelationalParsing.getLong;
import static com.hazelcast.jet.contrib.cdc.impl.RelationalParsing.getString;
import static com.hazelcast.jet.contrib.cdc.impl.RelationalParsing.parse;

public class ChangeEventValueRelationalImpl implements ChangeEventValue {

    private final String json;
    private final ThrowingSupplier<Optional<Long>, ParsingException> timestamp;
    private final ThrowingSupplier<Operation, ParsingException> operation;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> before;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> after;

    public ChangeEventValueRelationalImpl(String valueJson, ObjectMapper mapper) {
        Objects.requireNonNull(valueJson, "valueJson");
        Objects.requireNonNull(mapper, "mapper");

        ThrowingSupplier<JsonNode, ParsingException> node = parse(valueJson, mapper);
        this.timestamp = new LazyThrowingSupplier<>(() ->
                getLong(node.get(), "ts_ms"));
        this.operation = new LazyThrowingSupplier<>(() ->
                Operation.get(getString(node.get(), "op").orElse(null)));
        this.before = new LazyThrowingSupplier<>(() -> RelationalParsing.getNode(node.get(), "before", mapper).get()
                .map(n -> new ChangeEventElementRelationalImpl(n, mapper)));
        this.after = new LazyThrowingSupplier<>(() -> RelationalParsing.getNode(node.get(), "after", mapper).get()
                .map(n -> new ChangeEventElementRelationalImpl(n, mapper)));
        this.json = valueJson;
    }

    @Override
    public long timestamp() throws ParsingException {
        return timestamp.get().orElseThrow(() -> new ParsingException("No parsable timestamp field found"));
    }

    @Override
    public Operation getOperation() throws ParsingException {
        return operation.get();
    }

    @Override
    public ChangeEventElement before() throws ParsingException {
        return before.get().orElseThrow(() -> new ParsingException("No 'before' sub-node present"));
    }

    @Override
    public ChangeEventElement after() throws ParsingException {
        return after.get().orElseThrow(() -> new ParsingException("No 'after' sub-node present"));
    }

    @Override
    public ChangeEventElement change() {
        throw new UnsupportedOperationException("Not supported for relational databases");
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
