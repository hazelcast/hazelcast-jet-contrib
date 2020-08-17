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

package com.hazelcast.jet.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetClientConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @param <S> "SELF" to be used in the <code>withXXX</code> methods.
 */
public class JetContainer<S extends JetContainer<S>> extends GenericContainer<S> {


    /**
     * Default image to start
     */
    public static final String DEFAULT_IMAGE = "hazelcast/hazelcast-jet:4.2";

    /**
     * Default port Hazelcast is running on
     */
    public static final int HZ_PORT = 5701;

    /**
     * Default debug port
     */
    public static final int DEBUG_PORT = 5005;

    private final Set<String> jetModules = new HashSet<>();
    private final Set<String> cpItems = new HashSet<>();
    private final Set<String> javaOpts = new HashSet<>();

    private boolean debug;
    private int debugPort = DEBUG_PORT;
    private boolean debugSuspend;

    /**
     * Creates a container with the default image
     */
    public JetContainer() {
        this(DEFAULT_IMAGE);
    }

    /**
     * Create the container with given docker image name
     *
     * @param dockerImageName docker image name
     */
    public JetContainer(String dockerImageName) {
        super(dockerImageName);

        WaitStrategy waitForPort = new HostPortWaitStrategy();
        WaitStrategy waitForLog = new LogMessageWaitStrategy()
                .withRegEx(".*\\[.*\\..*\\..*\\..*]:.* is STARTED\n");

        this.waitStrategy = new WaitAllStrategy()
                .withStrategy(waitForPort)
                .withStrategy(waitForLog)
                .withStartupTimeout(Duration.ofMinutes(2));

        addExposedPort(HZ_PORT);
        javaOpts.add("-Dhazelcast.phone.home.enabled=false");
    }

    /**
     * Use provided file as a Hazelcast configuration file
     *
     * @param filePath path, relative or absolute
     */
    public S withHazelcastConfigurationFile(String filePath) {
        withFileSystemBind(filePath, "/opt/hazelcast-jet/config/hazelcast.yaml");
        return self();
    }

    /**
     * Use provided file as a Jet configuration file
     *
     * @param filePath path, relative or absolute
     */
    public S withJetConfigurationFile(String filePath) {
        withFileSystemBind(filePath, "/opt/hazelcast-jet/config/hazelcast-jet.yaml");
        return self();
    }

    /**
     * Enables one of the modules distributed with the official distribution
     *
     * @param jetModule name of the module
     */
    public S withJetModule(String jetModule) {
        jetModules.add(jetModule);
        return self();
    }

    /**
     *
     */
    public S withJarOnClassPath(String jarPath) {
        Path path = Paths.get(jarPath);
        String containerPath = "/opt/hazelcast-jet/ext/" + path.getFileName().toString();
        withFileSystemBind(jarPath, containerPath);
        cpItems.add(containerPath);
        return self();
    }

    /**
     * Use provided file as jvm.options file. The file must contain:
     * - One option per line
     * - Lines starting with # and empty lines are ignored
     *
     * @param filePath jvm options file path
     */
    public S withJvmOptionsFile(String filePath) {
        withFileSystemBind(filePath, "/opt/hazelcast-jet/config/jvm.options");
        return self();
    }

    /**
     * Enable debugging on port 5005
     * TODO this doesn't work correctly, currently it publishes the default port as random port on host,
     * logging the random port
     */
    public S withDebugEnabled() {
        this.debug = true;
        return self();
    }

    /**
     * Enable debugging on given port with specified suspend behaviour
     *
     * @param port    port number
     * @param suspend set to true to suspend the jvm until the debugger is connected
     */
    public S withDebugEnabled(int port, boolean suspend) {
        this.debug = true;
        this.debugPort = port;
        this.debugSuspend = suspend;
        return self();
    }

    /**
     * Returns a Jet client configured to connect to this member
     */
    public JetInstance getJetClient() {
        JetClientConfig config = new JetClientConfig();

        config.getNetworkConfig().addAddress(getContainerIpAddress() + ":" + getMappedPort(HZ_PORT));
        return Jet.newJetClient(config);
    }


    @Override
    protected void configure() {
        if (!jetModules.isEmpty()) {
            addEnv("JET_MODULES", String.join(",", jetModules));
        }
        if (!cpItems.isEmpty()) {
            addEnv("CLASSPATH", String.join(",", cpItems));
        }
        if (debug) {
            javaOpts.add("-agentlib:jdwp=transport=dt_socket,server=y," +
                    "suspend=" + (debugSuspend ? "y" : "n") +
                    ",address=*:" + debugPort);
            addExposedPort(debugPort);
            //            TODO doesn't work, bug?
            //            addFixedExposedPort(debugPort, debugPort);
        }

        addEnv("JAVA_OPTS", String.join(" ", javaOpts));
        super.configure();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        if (debug) {
            logger().info("Debug port: " + getMappedPort(debugPort));
        }
    }
}
