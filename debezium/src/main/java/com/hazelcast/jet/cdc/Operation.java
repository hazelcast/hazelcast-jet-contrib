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

/**
 * TODO: javadoc
 */
public enum Operation {
    /**
     * TODO: javadoc
     */
    SYNC("r"),
    /**
     * TODO: javadoc
     */
    INSERT("c"),
    /**
     * TODO: javadoc
     */
    UPDATE("u"),
    /**
     * TODO: javadoc
     */
    DELETE("d");

    private final String id;

    Operation(String id) {
        this.id = id;
    }

    /**
     * TODO: javadoc
     */
    public static Operation get(String id) {
        Operation[] values = values();
        for (Operation value : values) {
            if (value.id.equals(id)) {
                return value;
            }
        }
        throw new IllegalArgumentException(id + " is not a valid operation id");
    }
}
