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

import java.io.Serializable;
import java.util.Optional;

/**
 * TODO: javadoc
 */
public interface ChangeEventElement extends Serializable {

    //todo: use better serialization

    //todo: do we have enough getXXX() methods? can we completely parse all messages with them? TEST

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element is unparsable or the mapping
     *                          fails to produce a result
     */
    <T> T map(Class<T> clazz) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<Object> getObject(String key) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<String> getString(String key) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<Integer> getInteger(String key) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<Long> getLong(String key) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<Double> getDouble(String key) throws ParsingException;

    /**
     * TODO: javadoc
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    Optional<Boolean> getBoolean(String key) throws ParsingException;

    /**
     * Returns raw JSON string on which the content of this event is
     * based. To be used when parsing fails for some reason (for example
     * on some untested DB-connector version combination).
     * <p>
     * While the format is standard for RELATIONAL DATABASES, for
     * MongoDB it's MongoDB Extended JSON v2 format and needs to be
     * parsed accordingly.
     * TODO: javadoc
     */
    String asJson();

}
