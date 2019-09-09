package com.hazelcast.jet.contrib.http.marshalling.impl;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.contrib.http.marshalling.MarshallingStrategy;

/**
 * Marshalling strategy which parses JSON payload and returns a value object
 * which has convenience for accessing JSON fields.
 *
 * NOTE: This strategy uses Hazelcast's internal JSON classes which might subject
 * to change.
 */
public class HazelcastInternalJsonMarshallingStrategy implements MarshallingStrategy<JsonValue> {

    @Override
    public JsonValue getObject(String payload) {
        return Json.parse(payload);
    }
}
