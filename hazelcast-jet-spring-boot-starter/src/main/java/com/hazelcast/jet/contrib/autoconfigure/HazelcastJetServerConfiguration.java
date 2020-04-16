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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.contrib.autoconfigure.conditions.HazelcastJetConfigResourceCondition;
import com.hazelcast.jet.contrib.autoconfigure.conditions.HazelcastJetServerConfigAvailableCondition;
import com.hazelcast.jet.contrib.autoconfigure.properties.HazelcastJetIMDGProperty;
import com.hazelcast.jet.contrib.autoconfigure.properties.HazelcastJetServerProperty;
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
@EnableConfigurationProperties({HazelcastJetServerProperty.class, HazelcastJetIMDGProperty.class})
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

    private static Config getIMDGConfig(Resource configLocation) throws IOException {
        URL configUrl = configLocation.getURL();
        String configFileName = configUrl.getPath();
        InputStream inputStream = configUrl.openStream();
        if (configFileName.endsWith(".yaml") || configFileName.endsWith("yml")) {
            return new YamlConfigBuilder(inputStream).build();
        }
        return new XmlConfigBuilder(inputStream).build();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean({JetConfig.class, ClientConfig.class})
    @Conditional(HazelcastJetServerConfigAvailableCondition.class)
    static class HazelcastJetServerConfigFileConfiguration {

        @Autowired
        private ApplicationContext applicationContext;

        @Bean
        JetInstance jetInstance(HazelcastJetServerProperty serverProperty,
                                HazelcastJetIMDGProperty imdgProperty) throws IOException {
            Resource serverConfigLocation = serverProperty.resolveConfigLocation();
            Resource imdgConfigLocation = imdgProperty.resolveConfigLocation();

            JetConfig jetConfig = serverConfigLocation != null ? getJetConfig(serverConfigLocation)
                    : ConfigProvider.locateAndGetJetConfig();
            if (imdgConfigLocation != null) {
                jetConfig.setHazelcastConfig(getIMDGConfig(imdgConfigLocation));
            }

            injectSpringManagedContext(jetConfig.getHazelcastConfig());

            return Jet.newJetInstance(jetConfig);
        }

        private void injectSpringManagedContext(Config config) {
            SpringManagedContext springManagedContext = new SpringManagedContext();
            springManagedContext.setApplicationContext(applicationContext);
            config.setManagedContext(springManagedContext);
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
