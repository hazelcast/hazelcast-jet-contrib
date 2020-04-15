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

import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.contrib.autoconfigure.conditions.HazelcastJetServerConfigAvailableCondition;
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
        // hazelcast-jet.yaml and hazelcast.yaml present in root classpath
        contextRunner.run((context) -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            EdgeConfig defaultEdgeConfig = jetConfig.getDefaultEdgeConfig();
            Config hazelcastConfig = jetConfig.getHazelcastConfig();
            assertThat(defaultEdgeConfig.getQueueSize()).isEqualTo(2048);
            assertThat(hazelcastConfig.getClusterName()).isEqualTo("default-cluster");
        });
    }

    @Test
    public void systemPropertyWithXml() {
        contextRunner
                .withSystemProperties(HazelcastJetServerConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyClassPathWithXml() {
        contextRunner
                .withSystemProperties(HazelcastJetServerConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyWithYaml() {
        contextRunner
                .withSystemProperties(HazelcastJetServerConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void systemPropertyClassPathWithYaml() {
        contextRunner
                .withSystemProperties(HazelcastJetServerConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void configPropertyFileWithXml() {
        contextRunner
                .withPropertyValues(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void configPropertyFileWithYaml() {
        contextRunner
                .withPropertyValues(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void configPropertyClassPathUrlWithXml() {
        contextRunner
                .withPropertyValues(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY +
                        "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void configPropertyClassPathUrlWithYaml() {
        contextRunner
                .withPropertyValues(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY +
                        "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void unknownSystemPropertyConfigFile() {
        contextRunner
                .withSystemProperties(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY +
                        "=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                                                     .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void unknownConfigFile() {
        contextRunner
                .withPropertyValues(HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY +
                        "=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                                                     .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void configInstanceWithoutName() {
        contextRunner
                .withUserConfiguration(HazelcastJetConfig.class)
                .withPropertyValues("hazelcast.jet.config=this-is-ignored.xml")
                .run(assertSpecificJetServer("configAsBean"));
    }

    @Test
    public void systemPropertyImdgWithXml() {
        contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void systemPropertyClassPathImdgWithXml() {
        contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void systemPropertyImdgWithYaml() {
        contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void systemPropertyClassPathImdgWithYaml() {
        contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void configPropertyImdgFileWithXml() {
        contextRunner
                .withPropertyValues("hazelcast.jet.imdg.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                        + "autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void configPropertyImdgFileWithYaml() {
        contextRunner
                .withPropertyValues("hazelcast.jet.imdg.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                        + "autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void configPropertyImdgClassPathUrlWithXml() {
        contextRunner
                .withPropertyValues("hazelcast.jet.imdg.config=classpath:com/hazelcast/jet/contrib/"
                        + "autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void configPropertyImdgClassPathUrlWithYaml() {
        contextRunner
                .withPropertyValues("hazelcast.jet.imdg.config=classpath:com/hazelcast/jet/contrib/"
                        + "autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void unknownSystemPropertyConfigImdgFile() {
        contextRunner
                .withSystemProperties("hazelcast.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                                                     .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void unknownConfigPropertyImdgFile() {
        contextRunner
                .withPropertyValues("hazelcast.jet.imdg.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                                                     .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void configImdgInstanceWithoutName() {
        contextRunner
                .withUserConfiguration(HazelcastJetConfig.class)
                .withPropertyValues("hazelcast.jet.imdg.config=this-is-ignored.xml")
                .run(assertSpecificJetImdgServer("configAsBean-cluster"));
    }

    @Test
    public void systemProperty_bothConfigFile() {
        contextRunner
                .withSystemProperties(
                        HazelcastJetServerConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                                + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml",
                        "hazelcast.config"
                                + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(context -> {
                    assertSpecificJetServer("xml").accept(context);
                    assertSpecificJetImdgServer("explicit-xml-cluster").accept(context);
                });
    }

    @Test
    public void explicitConfigFile_bothConfigFile() {
        contextRunner
                .withPropertyValues(
                        HazelcastJetServerConfigAvailableCondition.CONFIG_ENVIRONMENT_PROPERTY
                                + "=com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml",
                        "hazelcast.jet.imdg.config"
                                + "=com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(context -> {
                    assertSpecificJetServer("yaml").accept(context);
                    assertSpecificJetImdgServer("explicit-yaml-cluster").accept(context);
                });

    }

    private static ContextConsumer<AssertableApplicationContext> assertSpecificJetServer(String suffix) {
        return context -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            assertThat(jetConfig.getProperties().getProperty("foo")).isEqualTo("bar-" + suffix);
        };
    }

    private static ContextConsumer<AssertableApplicationContext> assertSpecificJetImdgServer(String clusterName) {
        return context -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            assertThat(jetConfig.getHazelcastConfig().getClusterName()).isEqualTo(clusterName);
        };
    }

    @Configuration(proxyBeanMethods = false)
    static class HazelcastJetConfig {

        @Bean
        JetConfig anotherHazelcastJetConfig() {
            JetConfig jetConfig = new JetConfig();
            jetConfig.getHazelcastConfig().getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            jetConfig.setProperty("foo", "bar-configAsBean");
            jetConfig.getHazelcastConfig().setClusterName("configAsBean-cluster");
            return jetConfig;
        }

    }

}
