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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.contrib.cdc.ParsingException;
import com.hazelcast.jet.contrib.cdc.util.ThrowingSupplier;

import javax.ws.rs.ProcessingException;
import java.util.Optional;

public final class RelationalParsing {

    private RelationalParsing() {
    }

    public static ThrowingSupplier<JsonNode, ParsingException> parse(String json, ObjectMapper mapper) {
        return () -> {
            try {
                return mapper.readTree(json);
            } catch (Exception e) {
                throw new ProcessingException(e.getMessage(), e);
            }
        };
    }

    public static ThrowingSupplier<Optional<JsonNode>, ParsingException> getNode(
            JsonNode node, String key, ObjectMapper mapper) {
        return () -> {
            JsonNode subNode = node.get(key);
            if (subNode == null || subNode.isNull()) {
                return Optional.empty();
            } else {
                return Optional.of(subNode);
            }
        };
    }

    public static <T> T map(JsonNode node, Class<T> clazz, ObjectMapper mapper) throws ParsingException {
        try {
            return mapper.treeToValue(node, clazz);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    public static Optional<Object> getObject(JsonNode node, String key) {
        Object object = node.get(key);
        return Optional.ofNullable(object);
    }

    public static Optional<String> getString(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null && valueNode.isValueNode()) {
            return Optional.of(valueNode.asText());
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Integer> getInteger(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            if (valueNode.isNumber()) {
                return Optional.of(valueNode.asInt());
            } else {
                try {
                    return getString(node, key).map(Integer::parseInt);
                } catch (Exception e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<Long> getLong(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            if (valueNode.isNumber()) {
                return Optional.of(valueNode.asLong());
            } else {
                try {
                    return getString(node, key).map(Long::parseLong);
                } catch (Exception e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<Double> getDouble(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            if (valueNode.isNumber()) {
                return Optional.of(valueNode.asDouble());
            } else {
                try {
                    return getString(node, key).map(Double::parseDouble);
                } catch (Exception e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<Boolean> getBoolean(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            if (valueNode.isBoolean()) {
                return Optional.of(valueNode.asBoolean());
            } else {
                try {
                    return getString(node, key).map(Boolean::parseBoolean);
                } catch (Exception e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

}
