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

import com.hazelcast.jet.cdc.ChangeEventKey;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.util.ThrowingSupplier;
import org.bson.Document;

import java.util.Objects;

public class ChangeEventKeyMongoImpl implements ChangeEventKey {

    private final String json;
    private final ThrowingSupplier<Integer, ParsingException> id;

    public ChangeEventKeyMongoImpl(String keyJson) {
        Objects.requireNonNull(keyJson, "keyJson");

        this.json = keyJson;
        this.id = new LazyThrowingSupplier<>(() -> toId(keyJson));
    }

    @Override
    public int id() throws ParsingException {
        return id.get();
    }

    @Override
    public String asJson() {
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

    private static Integer toId(String keyJson) throws ParsingException {
        try {
            String stringId = Document.parse(keyJson).getString("id");
            return Integer.valueOf(stringId);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }
}
