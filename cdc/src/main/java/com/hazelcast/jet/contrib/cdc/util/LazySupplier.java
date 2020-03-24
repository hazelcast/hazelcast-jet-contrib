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

package com.hazelcast.jet.contrib.cdc.util;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * TODO: javadoc
 * @param <T>
 */
public class LazySupplier<T> implements Supplier<T> {

    @Nonnull
    private final Supplier<T> expensiveSupplier;

    private T value;

    /**
     * TODO: javadoc
     */
    public LazySupplier(@Nonnull Supplier<T> expensiveSupplier) {
        this.expensiveSupplier = Objects.requireNonNull(expensiveSupplier);
    }

    /**
     * TODO: javadoc
     */
    @Override
    public T get() {
        if (value == null) {
            value = expensiveSupplier.get();
        }
        return value;
    }
}
