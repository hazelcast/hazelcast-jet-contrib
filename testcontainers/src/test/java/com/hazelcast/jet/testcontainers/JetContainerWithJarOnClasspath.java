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

package com.hazelcast.jet.testcontainers;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JetContainerWithJarOnClasspath {

    @ClassRule
    public static JetContainer<?> jet = new JetContainer<>()
            .withJarOnClassPath("src/test/resources/empty.jar");

    @Test
    public void testJetStartsWithJarOnClassPath() {
        String logs = jet.getLogs();
        assertThat(logs).contains("/opt/hazelcast-jet/ext/empty.jar");
    }
}
