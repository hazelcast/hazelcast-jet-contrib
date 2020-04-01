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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.cdc.ChangeEvent;
import com.hazelcast.jet.contrib.cdc.ChangeEventKey;
import com.hazelcast.jet.contrib.cdc.ChangeEventValue;
import com.hazelcast.jet.contrib.cdc.util.LazySupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class ChangeEventJsonImpl implements ChangeEvent {

    private final SupplierEx<String> json;
    private final SupplierEx<ChangeEventKey> key;
    private final SupplierEx<ChangeEventValue> value;

    public ChangeEventJsonImpl(@Nullable String keyJson,
                               @Nullable String valueJson,
                               @Nonnull ObjectMapper mapper) {
        Objects.requireNonNull(keyJson, "keyJson");
        Objects.requireNonNull(valueJson, "valueJson");
        Objects.requireNonNull(mapper, "mapper");

        this.key = new LazySupplier<>(() -> new ChangeEventKeyJsonImpl(keyJson, mapper));
        this.value = new LazySupplier<>(() -> new ChangeEventValueJsonImpl(valueJson, mapper));
        this.json = new LazySupplier<>(() -> String.format("key:{%s}, value:{%s}", keyJson, valueJson));
    }

    @Override
    public ChangeEventKey key() {
        return key.get();
    }

    @Override
    public ChangeEventValue value() {
        return value.get();
    }

    @Override
    public String asJson() {
        return json.get();
    }

    @Override
    public String toString() {
        return asJson();
    }

}
