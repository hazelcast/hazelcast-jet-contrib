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
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import org.bson.Document;

public class ChangeEventKeyMongoImpl extends AbstractMongoFlatValues implements ChangeEventKey {

    private final String json;

    public ChangeEventKeyMongoImpl(String keyJson) {
        super(new LazyThrowingSupplier<>(() -> toDocument(keyJson)));
        this.json = keyJson;
    }

    public ChangeEventKeyMongoImpl(Document document) {
        super(() -> document);
        this.json = document.toJson();
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
}
