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

import com.hazelcast.jet.JetInstance;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class JetContainerWithDebugEnabled {

    @ClassRule
    public static JetContainer<?> jet = new JetContainer<>()
            .withDebugEnabled(5005, true);


    @Test
    @Ignore
    public void shouldStartClusterWithDebugEnabled() throws Exception {
        JetInstance client = jet.getJetClient();

        // now you can connect to the JVM in the container
        Thread.sleep(300_000);
    }

}
