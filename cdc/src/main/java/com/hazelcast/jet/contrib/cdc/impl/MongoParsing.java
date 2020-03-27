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

package com.hazelcast.jet.contrib.cdc.impl;

import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.LazyThrowingSupplier;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;
import org.bson.Document;

import java.util.Optional;

public final class MongoParsing {

    private MongoParsing() {
    }

    public static ThrowingSupplier<Document, ParsingException> parse(String json) {
        return () -> {
            try {
                return Document.parse(json);
            } catch (Exception e) {
                throw new ParsingException(e.getMessage(), e);
            }
        };
    }

    public static ThrowingSupplier<Optional<Document>, ParsingException> getDocument(Document parent, String key) {
        return new LazyThrowingSupplier<>(
                () -> {
                    Optional<String> json = getString(parent, key);
                    Document result;
                    try {
                        result = Document.parse(json.get());
                    } catch (Exception e) {
                        throw new ParsingException(e.getMessage(), e);
                    }
                    return json.isPresent() ? Optional.of(result) : Optional.empty();
                }
        );
    }

    public static Optional<Object> getObject(Document document, String key) {
        Object object = document.get(key);
        return Optional.ofNullable(object);
    }

    public static Optional<String> getString(Document document, String key) {
        Object object = document.get(key);
        if (object instanceof String) {
            return Optional.of((String) object);
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Integer> getInteger(Document document, String key) {
        Object object = document.get(key);
        if (object instanceof Number) {
            return Optional.of(((Number) object).intValue());
        } else if (object instanceof String) {
            try {
                return Optional.of(new Integer((String) object));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Long> getLong(Document document, String key) {
        Object object = document.get(key);
        if (object instanceof Number) {
            return Optional.of(((Number) object).longValue());
        } else if (object instanceof String) {
            try {
                return Optional.of(new Long((String) object));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Double> getDouble(Document document, String key) {
        Object object = document.get(key);
        if (object instanceof Number) {
            return Optional.of(((Number) object).doubleValue());
        } else if (object instanceof String) {
            try {
                return Optional.of(new Double((String) object));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Boolean> getBoolean(Document document, String key) {
        Object object = document.get(key);
        if (object instanceof Boolean) {
            return Optional.of(((Boolean) object));
        } else if (object instanceof String) {
            return Optional.of(Boolean.valueOf((String) object));
        } else {
            return Optional.empty();
        }
    }

}
