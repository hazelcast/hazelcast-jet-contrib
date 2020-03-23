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

package com.hazelcast.jet.cdc;

import java.util.Optional;

/**
 * TODO: javadoc
 */
public interface ChangeEventValue {

    /**
     * TODO: javadoc
     */
    Operation getOperation();

    /**
     * TODO: javadoc
     */
    <T> Optional<T> getBefore(Class<T> clazz) throws ParsingException;

    /**
     * TODO: javadoc
     */
    <T> Optional<T> getAfter(Class<T> clazz) throws ParsingException;

    /**
     * TODO: javadoc
     */
    <T> Optional<T> getCustom(String name, Class<T> clazz) throws ParsingException;

    /**
     * TODO: javadoc
     */
    default <T> T getLatest(Class<T> clazz) throws ParsingException {
        Optional<T> after = getAfter(clazz);
        if (after.isPresent()) {
            return after.get();
        }

        Optional<T> before = getBefore(clazz);
        if (before.isPresent()) {
            return before.get();
        }

        throw new IllegalStateException(ChangeEventValue.class.getSimpleName() +
                " should have either a before or after value");
    }
}
