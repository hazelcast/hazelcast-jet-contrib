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

package com.hazelcast.jet.contrib.autoconfigure.conditions;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * {@link HazelcastJetConfigResourceCondition} that checks if the
 * {@code hazelcast.jet.config} configuration key is defined.
 */
public class HazelcastJetServerConfigAvailableCondition extends HazelcastJetConfigResourceCondition {

    /**
     * Config property for Hazelcast Jet server configuration
     */
    public static final String CONFIG_ENVIRONMENT_PROPERTY = "hazelcast.jet.server.config";

    /**
     * System property for Hazelcast Jet server configuration
     */
    public static final String CONFIG_SYSTEM_PROPERTY = "hazelcast.jet.config";

    private final HazelcastJetClientConfigAvailableCondition clientConfigAvailableCondition =
            new HazelcastJetClientConfigAvailableCondition();

    /**
     * Creates a resource condition which checks if Hazelcast Jet server is
     * configured explicitly via system or environment properties and also
     * checks if the predefined configuration files are on the classpath or
     * root directory.
     * <p>
     * The resource condition also checks if Hazelcast Jet client is configured
     * explicitly via system or environment properties. The order of resource
     * checking is like below:
     * <ul>
     *     <li>
     *         If {@link #CONFIG_SYSTEM_PROPERTY} set, returns a match.
     *     </li>
     *     <li>
     *         If {@link #CONFIG_ENVIRONMENT_PROPERTY} set, returns a match.
     *     </li>
     *     <li>
     *         If {@link HazelcastJetClientConfigAvailableCondition#CONFIG_SYSTEM_PROPERTY}
     *         set, returns a no-match.
     *     </li>
     *     <li>
     *         If {@link HazelcastJetClientConfigAvailableCondition#CONFIG_ENVIRONMENT_PROPERTY}
     *         set, returns a no-match.
     *     </li>
     *     <li>
     *         If `hazelcast-jet.yaml`, `hazelcast-jet.yml` or
     *         `hazelcast-jet.xml` found on the classpath or root directory,
     *         returns a match.
     *     </li>
     *     <li>
     *         If `hazelcast-client.yaml`, `hazelcast-client.yml` or
     *         `hazelcast-client.xml` found on the classpath or root directory,
     *         returns a no-match.
     *     </li>
     *     <li>
     *         If none of the above conditions are matched returns a match
     *         which indicates the default configuration file from Hazelcast
     *         Jet distribution (`hazelcast-jet-default.yaml`) will be used.
     *     </li>
     * </ul>
     */
    public HazelcastJetServerConfigAvailableCondition() {
        super("HazelcastJet", CONFIG_ENVIRONMENT_PROPERTY, CONFIG_SYSTEM_PROPERTY,
                "file:./hazelcast-jet.xml", "classpath:/hazelcast-jet.xml",
                "file:./hazelcast-jet.yaml", "classpath:/hazelcast-jet.yaml",
                "file:./hazelcast-jet.yml", "classpath:/hazelcast-jet.yml");
    }

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        // System property for server configuration found, match
        if (System.getProperty(configSystemProperty) != null) {
            return ConditionOutcome.match(
                    startConditionMessage().because("System property '" + configSystemProperty + "' is set."));
        }
        // Environment property for server configuration found, match
        if (context.getEnvironment().containsProperty(CONFIG_ENVIRONMENT_PROPERTY)) {
            return ConditionOutcome.match(startConditionMessage()
                    .foundExactly("property " + CONFIG_ENVIRONMENT_PROPERTY));
        }
        // System property for client configuration found, no match
        if (System.getProperty(HazelcastJetClientConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY) != null) {
            return ConditionOutcome.noMatch(startConditionMessage().because("System property '"
                    + HazelcastJetClientConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY + "' is set."));
        }
        // Environment property for client configuration found, no match
        if (context.getEnvironment()
                   .containsProperty(HazelcastJetClientConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY)) {
            return ConditionOutcome.noMatch(startConditionMessage().because("Environment property '"
                    + HazelcastJetClientConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY + "' is set."));
        }
        ConditionOutcome resourceOutcome = getResourceOutcome(context, metadata);
        // Found a configuration file for server, match
        if (resourceOutcome.isMatch()) {
            return resourceOutcome;
        }
        // Found a configuration file for client, no match
        ConditionOutcome clientResourceOutcome = clientConfigAvailableCondition.getMatchOutcome(context, metadata);
        if (clientResourceOutcome.isMatch()) {
            return ConditionOutcome.noMatch(clientResourceOutcome.getConditionMessage());
        }
        return ConditionOutcome.match(startConditionMessage().because(
                "No configuration option found, using default configuration."));
    }
}
