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

package com.hazelcast.jet.contrib.actuate;

import com.hazelcast.cluster.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.jet.JetInstance;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link HazelcastJetHealthIndicator}.
 */
public class HazelcastJetHealthIndicatorTests {

    private final JetInstance jet = mock(JetInstance.class);

    @Test
    public void hazelcastJetUp() {
        mockJet(true);

        Health health = new HazelcastJetHealthIndicator(jet).health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertDetails(health);
    }

    @Test
    public void hazelcastJetDown() {
        mockJet(false);

        Health health = new HazelcastJetHealthIndicator(jet).health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertDetails(health);
    }

    private void mockJet(boolean status) {
        HazelcastInstance hazelcast = mock(HazelcastInstance.class);
        LifecycleService lifecycleService = mock(LifecycleService.class);
        Endpoint endpoint = mock(Endpoint.class);

        Mockito.when(jet.getHazelcastInstance()).thenReturn(hazelcast);
        Mockito.when(jet.getName()).thenReturn("jet0-instance");
        Mockito.when(hazelcast.getLocalEndpoint()).thenReturn(endpoint);
        Mockito.when(hazelcast.getLifecycleService()).thenReturn(lifecycleService);
        Mockito.when(endpoint.getUuid()).thenReturn(UUID.fromString("7581bb2f-879f-413f-b574-0071d7519eb0"));
        Mockito.when(lifecycleService.isRunning()).thenReturn(status);
    }

    private void assertDetails(Health health) {
        assertThat(health.getDetails()).containsOnlyKeys("name", "uuid")
                                       .containsEntry("name", "jet0-instance")
                                       .containsEntry("uuid", "7581bb2f-879f-413f-b574-0071d7519eb0");
    }

}
