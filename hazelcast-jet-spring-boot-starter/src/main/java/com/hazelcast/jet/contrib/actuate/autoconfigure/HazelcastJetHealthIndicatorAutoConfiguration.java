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

package com.hazelcast.jet.contrib.actuate.autoconfigure;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.contrib.actuate.HazelcastJetHealthIndicator;
import com.hazelcast.jet.contrib.autoconfigure.HazelcastJetAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.CompositeHealthContributorConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for
 * {@link HazelcastJetHealthIndicator}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(JetInstance.class)
@ConditionalOnBean(JetInstance.class)
@ConditionalOnEnabledHealthIndicator("hazelcast.jet")
@AutoConfigureAfter(HazelcastJetAutoConfiguration.class)
public class HazelcastJetHealthIndicatorAutoConfiguration
        extends CompositeHealthContributorConfiguration<HazelcastJetHealthIndicator, JetInstance> {



    /**
     * Creates a {@link HealthIndicator} using the provided Hazelcast Jet
     * instances.
     */
    @Bean
    @ConditionalOnMissingBean(name = {"hazelcastJetHealthIndicator", "hazelcastJetHealthContributor"})
    public HealthContributor hazelcastJetHealthContributor(Map<String, JetInstance> jetInstances) {
        return createContributor(jetInstances);
    }

}
