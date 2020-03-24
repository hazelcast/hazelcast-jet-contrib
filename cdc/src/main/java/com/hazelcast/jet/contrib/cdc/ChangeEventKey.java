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

/**
 * TODO: javadoc
 */
public interface ChangeEventKey {

    /**
     * TODO: javadoc
     */
    <T> T map(Class<T> clazz) throws ParsingException;

    /**
     * TODO: javadoc
     */
    int id(String name) throws ParsingException;

    /**
     * Returns raw JSON string on which the content of this event is
     * based. To be used when parsing fails for some reason (for example
     * on some untested DB-connector version combination).
     *
     * While the format is standard for RELATIONAL DATABASES, for
     * MongoDB it's MongoDB Extended JSON v2 format and needs to be
     * parsed accordingly.
     * TODO: javadoc
     */
    String asJson();
}
