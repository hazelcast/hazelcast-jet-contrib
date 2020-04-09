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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.net.URL;

/**
 * Configuration for Hazelcast Jet client.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(HazelcastJetClientProperties.class)
@ConditionalOnMissingBean(JetInstance.class)
public class HazelcastJetClientConfiguration {


    private static ClientConfig getClientConfig(Resource clientConfigLocation) throws IOException {
        URL configUrl = clientConfigLocation.getURL();
        String configFileName = configUrl.getPath();
        if (configFileName.endsWith(".yaml") || configFileName.endsWith("yml")) {
            return new YamlClientConfigBuilder(configUrl).build();
        }
        return new XmlClientConfigBuilder(configUrl).build();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(ClientConfig.class)
    @Conditional(HazelcastJetClientConfigAvailableCondition.class)
    static class HazelcastJetClientConfigFileConfiguration {

        @Bean
        JetInstance jetInstance(HazelcastJetClientProperties properties) throws IOException {
            Resource configLocation = properties.resolveConfigLocation();
            if (configLocation == null) {
                return Jet.newJetClient();
            }
            return Jet.newJetClient(getClientConfig(configLocation));
        }

    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnSingleCandidate(ClientConfig.class)
    static class HazelcastJetClientConfigConfiguration {

        @Bean
        JetInstance jetInstance(ClientConfig clientConfig) {
            return Jet.newJetClient(clientConfig);
        }

    }

}
