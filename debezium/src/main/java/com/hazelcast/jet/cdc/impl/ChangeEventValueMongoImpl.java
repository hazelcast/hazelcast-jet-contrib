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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.util.LazySupplier;
import org.bson.Document;

import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventValueMongoImpl implements ChangeEventValue {

    //todo: is the mongodb java driver dependency a problem?
    //todo: use POJO from BSON instead of Document?

    private final Supplier<Operation> operation;
    private final Supplier<Optional<Document>> before;
    private final Supplier<Optional<Document>> after;
    private final Supplier<Optional<Document>> patch;
    private final Supplier<String> printForm;

    public ChangeEventValueMongoImpl(String valueJson) {
        Document document = Document.parse(valueJson);
        this.operation = new LazySupplier<>(() -> Operation.get(document.getString("op")));
        this.before = new LazySupplier<>(() -> subDocument(document, "before"));
        this.after = new LazySupplier<>(() -> subDocument(document, "after"));
        this.patch = new LazySupplier<>(() -> subDocument(document, "patch"));
        this.printForm = () -> valueJson;
    }

    private static boolean isNullNode(JsonNode node) {
        if (node == null) {
            return true;
        }
        JsonNodeType nodeType = node.getNodeType();
        if (nodeType == null) {
            return true;
        }
        return JsonNodeType.NULL.equals(nodeType);
    }

    @Override
    public Operation getOperation() {
        return operation.get();
    }

    @Override
    public <T> T getImage(Class<T> clazz) {
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
    public <T> T getUpdate(Class<T> clazz) {
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
    public String toString() {
        return printForm.get();
    }

    private Optional<Document> subDocument(Document parent, String key) {
        String json = parent.getString(key);
        return Optional.ofNullable(json == null ? null : Document.parse(json));
    }

}
