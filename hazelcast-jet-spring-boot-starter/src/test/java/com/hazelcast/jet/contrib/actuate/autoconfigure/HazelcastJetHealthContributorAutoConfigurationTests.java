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

import com.hazelcast.jet.contrib.autoconfigure.HazelcastJetAutoConfiguration;
import org.junit.Test;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.hazelcast.HazelcastHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HazelcastJetHealthIndicatorAutoConfiguration}.
 */
public class HazelcastJetHealthContributorAutoConfigurationTests {

    private ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HazelcastJetAutoConfiguration.class,
                    HazelcastJetHealthIndicatorAutoConfiguration.class, HealthEndpointAutoConfiguration.class));

    @Test
    public void runShouldCreateIndicator() {
        this.contextRunner.run((context) -> assertThat(context).hasSingleBean(HazelcastHealthIndicator.class));
    }

    @Test
    public void runWhenDisabledShouldNotCreateIndicator() {
        this.contextRunner.withPropertyValues("management.health.hazelcast.jet.enabled:false")
                          .run((context) -> assertThat(context).doesNotHaveBean(HazelcastHealthIndicator.class));
    }

}
