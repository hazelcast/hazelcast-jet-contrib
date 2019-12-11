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

package com.hazelcast.jet.contrib.probabilistic;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.spi.impl.SerializationServiceSupport;

/**
 * Utility for calculating hashes from arbitrary objects.
 *
 * It relies on Hazelcast serialization. It means it can calculate hashes from all objects as long
 * as they are serializable by Hazelcast.
 *
 */
public final class HashingSupport {
    private HashingSupport() {

    }

    /**
     * Creates a new factory for hashing service.
     *
     * @return factory for hashing service
     */
    public static ServiceFactory<HashingContext> hashingServiceFactory() {
        return ServiceFactory.withCreateFn(jet -> {
            SerializationServiceSupport support = (SerializationServiceSupport) jet.getHazelcastInstance();
            SerializationService serializationService = support.getSerializationService();
            return new HashingContext(serializationService);
        }).withLocalSharing();
    }

    /**
     * Calculate 64 bit hash. It's meant to work together with a factory returned by {@link #hashingServiceFactory()}
     *
     * @param <T> type of the element to calculate hash from.uses Hazelcast internal
     * @return function to calculate hash.
     */
    public static <T> BiFunctionEx<HashingContext, T, Long> hashingFn() {
        return HashingContext::hash64;
    }

    /**
     * Context for object hashing
     *
     */
    public static final class HashingContext {
        private final SerializationService serializationService;

        private HashingContext(SerializationService serializationService) {
            this.serializationService = serializationService;
        }

        private long hash64(Object object) {
            Data data = serializationService.toData(object);
            return data.hash64();
        }
    }

}
