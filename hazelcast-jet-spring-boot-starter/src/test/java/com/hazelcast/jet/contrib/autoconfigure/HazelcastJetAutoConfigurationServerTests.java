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
import org.junit.Ignore;
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
        // hazelcast-jet.yaml present in root classpath
        this.contextRunner.run((context) -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            EdgeConfig defaultEdgeConfig = jetConfig.getDefaultEdgeConfig();
            assertThat(defaultEdgeConfig.getQueueSize()).isEqualTo(2048);
        });
    }

    @Test
    public void systemPropertyWithXml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyClassPathWithXml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyFileWithXml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void systemPropertyWithYaml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void systemPropertyClassPathWithYaml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void systemPropertyFileWithYaml() {
        this.contextRunner
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void explicitConfigFileWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=com/hazelcast/jet/contrib/autoconfigure/"
                + "hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void explicitConfigFileWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=com/hazelcast/jet/contrib/autoconfigure/"
                + "hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void explicitConfigClassPathUrlWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void explicitConfigClassPathUrlWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void explicitConfigFileUrlWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    public void explicitConfigFileUrlWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-jet-specific.yaml")
                .run(assertSpecificJetServer("yaml"));
    }

    @Test
    public void unknownSystemPropertyConfigFile() {
        this.contextRunner.withSystemProperties("hazelcast.jet.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void unknownConfigFile() {
        this.contextRunner.withPropertyValues("hazelcast.jet.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void configInstanceWithoutName() {
        this.contextRunner.withUserConfiguration(HazelcastJetConfig.class)
                          .withPropertyValues("hazelcast.jet.config=this-is-ignored.xml")
                          .run(assertSpecificJetServer("configAsBean"));
    }

    @Test
    public void defaultImdgConfigFile() {
        // hazelcast.yaml present in root classpath
        this.contextRunner
                .run(assertSpecificJetImdgServer("default-cluster"));
    }

    @Test
    public void systemPropertyImdgWithXml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void systemPropertyClassPathImdgWithXml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/73")
    public void systemPropertyFileImdgWithXml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    public void systemPropertyImdgWithYaml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void systemPropertyClassPathImdgWithYaml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=classpath:com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/73")
    public void systemPropertyFileImdgWithYaml() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=file:src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgFileWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.config=src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgFileWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.config=src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgClassPathUrlWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgClassPathUrlWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.config=classpath:com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgFileUrlWithXml() {
        this.contextRunner.withPropertyValues("hazelcast.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigImdgFileUrlWithYaml() {
        this.contextRunner.withPropertyValues("hazelcast.config=file:src/test/resources/com/hazelcast/jet/contrib/"
                + "autoconfigure/hazelcast-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"));
    }

    @Test
    public void unknownSystemPropertyConfigImdgFile() {
        this.contextRunner.withSystemProperties("hazelcast.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void unknownConfigImdgFile() {
        this.contextRunner.withPropertyValues("hazelcast.config=foo/bar/unknown.xml")
                .run((context) -> assertThat(context).getFailure().isInstanceOf(BeanCreationException.class)
                .hasMessageContaining("foo/bar/unknown.xml"));
    }

    @Test
    public void configImdgInstanceWithoutName() {
        this.contextRunner.withUserConfiguration(HazelcastJetConfig.class)
                .withPropertyValues("hazelcast.config=this-is-ignored.xml")
                .run(assertSpecificJetImdgServer("configAsBean-cluster"));
    }

    @Test
    public void systemProperty_bothConfigFile() {
        this.contextRunner
                .withSystemProperties("hazelcast.config"
                        + "=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/hazelcast-specific.xml")
                .withSystemProperties(HazelcastJetConfigAvailableCondition.CONFIG_SYSTEM_PROPERTY
                        + "=com/hazelcast/jet/contrib/autoconfigure/hazelcast-jet-specific.xml")
                .run(assertSpecificJetImdgServer("explicit-xml-cluster"))
                .run(assertSpecificJetServer("xml"));
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast-jet-contrib/issues/71")
    public void explicitConfigFile_bothConfigFile() {
        this.contextRunner
                .withPropertyValues("hazelcast.config=src/test/resources/com/hazelcast/jet/contrib/autoconfigure/"
                        + "hazelcast-specific.yaml")
                .withPropertyValues("hazelcast.jet.config=com/hazelcast/jet/contrib/autoconfigure/"
                        + "hazelcast-jet-specific.yaml")
                .run(assertSpecificJetImdgServer("explicit-yaml-cluster"))
                .run(assertSpecificJetServer("yaml"));

    }

    private static ContextConsumer<AssertableApplicationContext> assertSpecificJetServer(String suffix) {
        return (context) -> {
            JetConfig jetConfig = context.getBean(JetInstance.class).getConfig();
            assertThat(jetConfig.getProperties().getProperty("foo")).isEqualTo("bar-" + suffix);
        };
    }

    private static ContextConsumer<AssertableApplicationContext> assertSpecificJetImdgServer(String clusterName) {
        return (context) -> {
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
