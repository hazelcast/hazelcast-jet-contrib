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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ContextConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HazelcastJetServerConfiguration}.
 */
public class HazelcastJetAutoConfigurationServerTests {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HazelcastJetAutoConfiguration.class));

    @Test
    public void defaultConfigFile() {
        // hazelcast-jet.xml present in root classpath
        this.contextRunner.run((context) -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            EdgeConfig defaultEdgeConfig = jetConfig.getDefaultEdgeConfig();
            assertThat(defaultEdgeConfig.getQueueSize()).isEqualTo(2048);
        });
    }

    @Test
    public void systemPropertyWithXml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetServerConfiguration.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyWithYaml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetServerConfiguration.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void explicitConfigFileWithXml() {
        this.contextRunner.withPropertyValues("spring.hazelcast.jet.config=com/hazelcast/jet/contrib/autoconfigure/"
                + "hazelcast-jet-specific.xml").run(assertSpecificJetServer("xml"));
    }

    @Test
    public void explicitConfigFileWithYaml() {
        this.contextRunner.withPropertyValues("spring.hazelcast.jet.config=com/hazelcast/jet/contrib/autoconfigure/"
                + "hazelcast-jet-specific.yaml").run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void explicitConfigUrlWithXml() {
        this.contextRunner.withPropertyValues("spring.hazelcast.jet.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.xml").run(assertSpecificJetServer("xml"));
    }

    @Test
    public void explicitConfigUrlWithYaml() {
        this.contextRunner.withPropertyValues("spring.hazelcast.jet.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.yaml").run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void unknownConfigFile() {
        this.contextRunner.withPropertyValues("spring.hazelcast.jet.config=foo/bar/unknown.xml")
                          .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                                                               .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void configInstanceWithoutName() {
        this.contextRunner.withUserConfiguration(HazelcastJetConfig.class)
                          .withPropertyValues("spring.hazelcast.jet.config=this-is-ignored.xml")
                          .run(assertSpecificJetServer("configAsBean"));
    }

    private static ContextConsumer<AssertableApplicationContext> assertSpecificJetServer(String suffix) {
        return (context) -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            assertThat(jetConfig.getProperties().getProperty("foo")).isEqualTo("bar-" + suffix);
        };
    }

    @Configuration(proxyBeanMethods = false)
    static class HazelcastJetConfig {

        @Bean
        JetConfig anotherHazelcastJetConfig() {
            JetConfig jetConfig = new JetConfig();
            jetConfig.getHazelcastConfig().getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            jetConfig.setProperty("foo", "bar-configAsBean");
            return jetConfig;
        }

    }

}
