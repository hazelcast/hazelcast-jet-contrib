package com.hazelcast.jet.contrib.http.marshalling.impl;

import com.hazelcast.jet.contrib.http.marshalling.MarshallingStrategy;

/**
 * Marshalling strategy which returns the HTTP message body string.
 */
public class RawStringMarshallingStrategy implements MarshallingStrategy<String> {

    @Override
    public String getObject(String payload) {
        return payload;
    }
}
