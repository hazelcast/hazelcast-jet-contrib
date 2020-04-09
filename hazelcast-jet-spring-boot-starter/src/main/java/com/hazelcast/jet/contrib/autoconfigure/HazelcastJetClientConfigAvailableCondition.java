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

package com.hazelcast.jet.contrib.autoconfigure;

/**
 * {@link HazelcastJetConfigResourceCondition} that checks if the
 * {@code hazelcast.jet.config} configuration key is defined.
 */
public class HazelcastJetClientConfigAvailableCondition extends HazelcastJetConfigResourceCondition {

    /**
     * Spring property for Hazelcast Jet client configuration
     */
    public static final String CONFIG_ENVIRONMENT_PROPERTY = "hazelcast.jet.client.config";

    /**
     * System property for Hazelcast Jet client configuration
     */
    public static final String CONFIG_SYSTEM_PROPERTY = "hazelcast.client.config";

    /**
     * Creates a resource condition which checks if Hazelcast Jet client is
     * configured explicitly via system or environment properties and also
     * checks if the predefined configuration files are on the classpath or
     * root directory.
     */
    public HazelcastJetClientConfigAvailableCondition() {
        super("HazelcastJetClient", CONFIG_ENVIRONMENT_PROPERTY, CONFIG_SYSTEM_PROPERTY,
                "file:./hazelcast-client.xml", "classpath:/hazelcast-client.xml",
                "file:./hazelcast-client.yaml", "classpath:/hazelcast-client.yaml",
                "file:./hazelcast-client.yml", "classpath:/hazelcast-client.yml");
    }
}
