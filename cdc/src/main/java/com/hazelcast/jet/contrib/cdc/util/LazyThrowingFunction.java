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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * TODO: javadoc
 * @param <T>
 * @param <R>
 * @param <E>
 */
public class LazyThrowingFunction<T, R, E extends Exception> implements ThrowingFunction<T, R, E> {

    @Nonnull
    private final ThrowingFunction<T, R, E> expensiveFunction;

    private transient Map<T, R> values = new HashMap<>();

    /**
     * TODO: javadoc
     */
    public LazyThrowingFunction(@Nonnull ThrowingFunction<T, R, E> expensiveFunction) {
        this.expensiveFunction = Objects.requireNonNull(expensiveFunction);
    }

    /**
     * TODO: javadoc
     */
    @Override
    public R apply(T t) throws E {
        R value = values.get(t);
        if (value == null) {
            value = expensiveFunction.apply(t);
            values.put(t, value);
        }
        return value;
    }
}
