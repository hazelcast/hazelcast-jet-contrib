package com.hazelcast.jet.contrib.http.marshalling;


import com.hazelcast.jet.contrib.http.marshalling.impl.HazelcastInternalJsonMarshallingStrategy;
import com.hazelcast.jet.contrib.http.marshalling.impl.RawStringMarshallingStrategy;

import java.io.Serializable;

/**
 * Allows one to de-serialize
 *
 * See {@link HazelcastInternalJsonMarshallingStrategy} and {@link RawStringMarshallingStrategy}
 */
public interface MarshallingStrategy<T> extends Serializable {
    /**
     * Takes a JSON payload, parses and returns an user object.
     * <p>
     * The returned user object should implement {@link Serializable} interface.
     *
     * @param payload string containing the JSON.
     * @return parsed user object from
     */
    T getObject(String payload);
}
