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

package com.hazelcast.jet.contrib.mqtt;

import org.testcontainers.utility.MountableFile;

import static com.hazelcast.jet.contrib.mqtt.MosquittoContainer.PORT;

public class SecuredMosquittoContainer extends MosquittoContainer {

    public static final String USERNAME = "myUser";
    public static final String PASSWORD = "myPassword";

    private static final String SECURED_CONFIG_FILE = "mosquitto_secured.conf";
    private static final String PWD_FILE = "passwordfile";

    public SecuredMosquittoContainer() {
        super();
    }

    @Override
    protected void configure() {
        addExposedPort(PORT);
        withCopyFileToContainer(MountableFile.forClasspathResource(PWD_FILE), "/");
        withCopyFileToContainer(MountableFile.forClasspathResource(SECURED_CONFIG_FILE), "/")
                .withCommand("mosquitto", "-c", SECURED_CONFIG_FILE);
    }
}
