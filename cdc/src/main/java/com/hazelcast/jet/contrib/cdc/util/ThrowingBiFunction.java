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

import java.io.Serializable;

/**
 * Serializable function (with two arguments), one that can throw an
 * {@code Exception} of an explicitly specified type from its
 * {@code apply()} method.
 * <p>
 * <b>NOT</b> thread safe.
 *
 * @param <T> type of first function parameter
 * @param <U> type of second function parameter
 * @param <R> type of function result
 * @param <E> type of thrown exception
 *
 * @since 4.1
 */
@FunctionalInterface
public interface ThrowingBiFunction<T, U, R, E extends Exception> extends Serializable {

    /**
     * Computes a result base on the two inputs, potentially throwing an
     * {@code E extends Exception} during the process.
     */
    R apply(T t, U u) throws E;

}
