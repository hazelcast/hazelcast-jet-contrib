/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.contrib.redis;

import io.lettuce.core.RedisClient;
import org.testcontainers.containers.GenericContainer;

import java.util.Collections;
import java.util.Set;

public class RedisContainer<SELF extends RedisContainer<SELF>> extends GenericContainer<SELF> {

    public static final String VERSION = "5.0.5";
    public static final Integer REDIS_PORT = 6379;

    private static final String IMAGE_NAME = "redis";

    public RedisContainer() {
        this(VERSION);
    }

    public RedisContainer(String version) {
        super(IMAGE_NAME + ":" + version);
    }

    @Override
    protected void configure() {
        addExposedPort(REDIS_PORT);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(mappedPort());
    }

    public int mappedPort() {
        return getMappedPort(REDIS_PORT);
    }

    /**
     * @return the connection string to Redis
     */
    public String connectionString() {
        return "redis://" + getContainerIpAddress() + ":" + mappedPort() + "/0";
    }

    /**
     * @return a new Redis client
     */
    public RedisClient newRedisClient() {
        return RedisClient.create(connectionString());
    }
}
