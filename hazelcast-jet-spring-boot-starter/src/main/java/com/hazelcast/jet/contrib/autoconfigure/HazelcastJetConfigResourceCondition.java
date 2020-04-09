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

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ResourceCondition;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.Assert;

/**
 * {@link SpringBootCondition} used to check if the Hazelcast Jet configuration is
 * available. This either kicks in if a default configuration has been found or if
 * configurable property referring to the resource to use has been set.
 */
public abstract class HazelcastJetConfigResourceCondition extends ResourceCondition {

    final String configSystemProperty;

    protected HazelcastJetConfigResourceCondition(String name, String property, String configSystemProperty,
                                                  String... resourceLocations) {
        super(name, property, resourceLocations);
        Assert.notNull(configSystemProperty, "ConfigSystemProperty must not be null");
        this.configSystemProperty = configSystemProperty;
    }

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        if (System.getProperty(this.configSystemProperty) != null) {
            return ConditionOutcome.match(
                    startConditionMessage().because("System property '" + this.configSystemProperty + "' is set."));
        }
        return super.getMatchOutcome(context, metadata);
    }

}
