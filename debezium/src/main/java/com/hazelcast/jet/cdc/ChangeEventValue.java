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

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * TODO: javadoc
 */
public interface ChangeEventValue {

    /**
     * TODO: javadoc
     */
    String getOperation();

    /**
     * TODO: javadoc
     */
    <T> T getBefore(Class<T> clazz) throws JsonProcessingException;

    /**
     * TODO: javadoc
     */
    <T> T getAfter(Class<T> clazz) throws JsonProcessingException;

}
