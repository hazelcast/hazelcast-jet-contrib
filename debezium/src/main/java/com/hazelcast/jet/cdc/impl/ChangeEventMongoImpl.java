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

import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventKey;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.util.LazySupplier;
import com.hazelcast.jet.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.util.ThrowingSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

public class ChangeEventMongoImpl implements ChangeEvent {

    private final ThrowingSupplier<Optional<ChangeEventKey>, ParsingException> key;
    private final ThrowingSupplier<Optional<ChangeEventValue>, ParsingException> value;
    private final Supplier<String> printForm;

    public ChangeEventMongoImpl(@Nullable String keyJson, @Nullable String valueJson) {
        this.key = new LazyThrowingSupplier<>(() -> getChangeEventKey(keyJson));
        this.value = new LazyThrowingSupplier<>(() -> getChangeEventValue(valueJson));
        this.printForm = new LazySupplier<>(() -> String.format("key:{%s}, value:{%s}", keyJson, valueJson));
    }

    @Override
    public ChangeEventKey key() throws ParsingException {
        return key.get().get();
    }

    @Override
    public Optional<ChangeEventValue> value() throws ParsingException {
        return value.get();
    }

    @Override
    public String toString() {
        return printForm.get();
    }

    @Nonnull
    private static Optional<ChangeEventKey> getChangeEventKey(String keyJson) {
        return Optional.ofNullable(keyJson == null ? null : new ChangeEventKeyMongoImpl(keyJson));
    }

    @Nonnull
    private static Optional<ChangeEventValue> getChangeEventValue(String valueJson) {
        return Optional.ofNullable(valueJson == null ? null : new ChangeEventValueMongoImpl(valueJson));
    }

}
