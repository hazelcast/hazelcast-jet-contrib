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
import org.junit.Test;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link HazelcastJetHealthIndicatorAutoConfiguration}.
 */
public class HazelcastJetHealthContributorAutoConfigurationIntegrationTests {

    private ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HazelcastJetHealthIndicatorAutoConfiguration.class,
                    HazelcastJetAutoConfiguration.class, HealthEndpointAutoConfiguration.class));

    @Test
    public void hazelcastJetUp() {
        this.contextRunner.run((context) -> {
            assertThat(context).hasSingleBean(JetInstance.class).hasSingleBean(HazelcastJetHealthIndicator.class);
            JetInstance jet = context.getBean(JetInstance.class);
            Health health = context.getBean(HazelcastJetHealthIndicator.class).health();
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails())
                    .containsOnlyKeys("name", "uuid")
                    .containsEntry("name", jet.getName())
                    .containsEntry("uuid", jet.getHazelcastInstance().getLocalEndpoint().getUuid().toString());
        });
    }

    @Test
    public void hazelcastJetDown() {
        this.contextRunner.run((context) -> {
            context.getBean(JetInstance.class).shutdown();
            assertThat(context).hasSingleBean(HazelcastJetHealthIndicator.class);
            Health health = context.getBean(HazelcastJetHealthIndicator.class).health();
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        });
    }

}
