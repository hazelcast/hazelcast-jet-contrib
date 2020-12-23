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

import com.github.dockerjava.api.DockerClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testcontainers.utility.MountableFile;

import java.util.Collections;
import java.util.Set;

/**
 * todo add proper javadoc
 */
public class MosquittoContainer extends GenericContainer<MosquittoContainer> {

    public static final String VERSION = "1.6.9";
    public static final Integer PORT = 1883;

    private static final String IMAGE_NAME = "eclipse-mosquitto";
    private static final String CONFIG_FILE = "mosquitto.conf";

    public MosquittoContainer() {
        super(IMAGE_NAME + ":" + VERSION);
    }

    @Override
    protected void configure() {
        addExposedPort(PORT);
        String configFile = configFile();
        withCopyFileToContainer(MountableFile.forClasspathResource(configFile), "/")
                .withCommand("mosquitto", "-c", configFile);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(PORT));
    }

    protected String configFile() {
        return CONFIG_FILE;
    }

    /**
     * @return the connection string to Mosquitto
     */
    public String connectionString() {
        return "tcp://" + getContainerIpAddress() + ":" + getMappedPort(PORT);
    }

    /**
     * Fixes currently mapped port, executes a graceful stop command on
     * the container, stops  and removes the container, and starts the
     * container.
     */
    public void restart() {
        // fix the port number
        setPortBindings(Collections.singletonList(getMappedPort(PORT) + ":" + PORT));
        // graceful stop
        executeStopCommand();
        // terminate
        stop();
        // start
        start();
    }

    /**
     * Executes a graceful stop command
     */
    private void executeStopCommand() {
        DockerClient dockerClient = DockerClientFactory.instance().client();
        dockerClient.stopContainerCmd(getContainerId()).exec();
    }

    public static String connectionString(ContainerProxy containerProxy) {
        return "tcp://" + containerProxy.getContainerIpAddress() + ":" + containerProxy.getProxyPort();
    }
}
