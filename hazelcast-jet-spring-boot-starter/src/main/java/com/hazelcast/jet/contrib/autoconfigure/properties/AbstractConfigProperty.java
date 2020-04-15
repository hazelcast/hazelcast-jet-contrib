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

package com.hazelcast.jet.contrib.autoconfigure.properties;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * todo add proper javadoc
 */
public abstract class AbstractConfigProperty {

    /**
     * The location of the server configuration file to use to initialize
     * Hazelcast Jet.
     */
    private Resource config;

    /**
     * @return the location of the server configuration file to use to
     * initialize Hazelcast Jet.
     */
    public Resource getConfig() {
        return this.config;
    }

    /**
     * @param config The location of the server configuration file to use to
     *               initialize Hazelcast Jet.
     */
    public void setConfig(Resource config) {
        this.config = config;
    }

    /**
     * Resolve the config location if set.
     *
     * @return the location or {@code null} if it is not set
     * @throws IllegalArgumentException if the config attribute is set to an unknown
     *                                  location
     */
    public Resource resolveConfigLocation() {
        if (config == null) {
            return null;
        }
        Assert.isTrue(config.exists(),
                () -> "Hazelcast Jet " + name() + " configuration does not exist '" + config.getDescription() + "'");
        return config;
    }

    /**
     * @return the name of the property
     */
    public abstract String name();
}
