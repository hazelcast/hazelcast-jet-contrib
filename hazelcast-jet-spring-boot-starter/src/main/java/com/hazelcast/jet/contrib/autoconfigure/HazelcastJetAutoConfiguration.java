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
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Hazelcast Jet. Creates a
 * {@link JetInstance} based on explicit configuration or when a default configuration
 * file is found in the environment.
 *
 * @see HazelcastJetConfigResourceCondition
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(JetInstance.class)
@Import({ HazelcastJetServerConfiguration.class, HazelcastJetClientConfiguration.class })
public class HazelcastJetAutoConfiguration {
}
