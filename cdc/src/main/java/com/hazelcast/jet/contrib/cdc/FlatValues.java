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

package com.hazelcast.jet.contrib.cdc;

import java.util.Optional;

/**
 * Throws exception only if JSON has a wrong format, not when key missing.
 * TODO: javadoc
 */
public interface FlatValues {

    /**
     * TODO: javadoc
     */
    Optional<Object> getObject(String key) throws ParsingException ;

    /**
     * TODO: javadoc
     */
    Optional<String> getString(String key) throws ParsingException ;

    /**
     * TODO: javadoc
     */
    Optional<Integer> getInteger(String key) throws ParsingException;

    /**
     * TODO: javadoc
     */
    Optional<Long> getLong(String key) throws ParsingException ;

    /**
     * TODO: javadoc
     */
    Optional<Double> getDouble(String key) throws ParsingException ;

    /**
     * TODO: javadoc
     */
    Optional<Boolean> getBoolean(String key) throws ParsingException ;

}
