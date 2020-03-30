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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.spring.context.SpringManagedContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Hazelcast Jet. Creates a
 * {@link JetInstance} based on explicit configuration or when a default configuration
 * file is found in the environment.
 *
 * @see HazelcastJetConfigResourceCondition
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(HazelcastJetProperties.class)
@ConditionalOnMissingBean(JetInstance.class)
public class HazelcastJetServerConfiguration {

    private static JetConfig getJetConfig(Resource configLocation) throws IOException {
        URL configUrl = configLocation.getURL();
        String configFileName = configUrl.getPath();
        InputStream inputStream = configUrl.openStream();
        if (configFileName.endsWith(".yaml") || configFileName.endsWith("yml")) {
            return new YamlJetConfigBuilder(inputStream).build();
        }
        return new XmlJetConfigBuilder(inputStream).build();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(JetConfig.class)
    @Conditional(HazelcastJetConfigAvailableCondition.class)
    static class HazelcastJetServerConfigFileConfiguration {

        @Autowired
        private ApplicationContext applicationContext;

        @Bean
        JetInstance jetInstance(HazelcastJetProperties properties) throws IOException {
            Resource configLocation = properties.resolveConfigLocation();
            JetConfig jetConfig = (configLocation != null) ? getJetConfig(configLocation)
                    : ConfigProvider.locateAndGetJetConfig();
            injectSpringManagedContext(jetConfig);
            return Jet.newJetInstance(jetConfig);
        }

        private void injectSpringManagedContext(JetConfig jetConfig) {
            SpringManagedContext springManagedContext = new SpringManagedContext();
            springManagedContext.setApplicationContext(this.applicationContext);
            jetConfig.getHazelcastConfig().setManagedContext(springManagedContext);
        }

    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnSingleCandidate(JetConfig.class)
    static class HazelcastJetServerConfigConfiguration {

        @Bean
        JetInstance jetInstance(JetConfig jetConfig) {
            return Jet.newJetInstance(jetConfig);
        }

    }

}
