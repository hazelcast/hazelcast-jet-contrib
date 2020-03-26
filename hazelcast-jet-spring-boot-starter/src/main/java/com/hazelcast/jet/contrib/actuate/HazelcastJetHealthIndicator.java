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

import com.hazelcast.core.LifecycleService;
import com.hazelcast.jet.JetInstance;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.util.Assert;

/**
 * {@link HealthIndicator} for Hazelcast Jet.
 */
public class HazelcastJetHealthIndicator extends AbstractHealthIndicator {

    private final JetInstance jetInstance;

    /**
     * Creates an health indicator for Hazelcast Jet which reports the status
     * of the {@link LifecycleService}.
     */
    public HazelcastJetHealthIndicator(JetInstance jetInstance) {
        super("Hazelcast Jet health check failed");
        Assert.notNull(jetInstance, "JetInstance must not be null");
        this.jetInstance = jetInstance;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        if (jetInstance.getHazelcastInstance().getLifecycleService().isRunning()) {
            builder.up();
        } else {
            builder.down();
        }
        builder.withDetail("name", jetInstance.getName()).withDetail("uuid",
                jetInstance.getHazelcastInstance().getLocalEndpoint().getUuid().toString());
    }
}
