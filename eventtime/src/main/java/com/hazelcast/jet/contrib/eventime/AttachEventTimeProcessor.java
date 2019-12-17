/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.contrib.eventime;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.impl.JetEvent;

final class AttachEventTimeProcessor<I, O> extends AbstractProcessor {
    private final BiFunctionEx<? super I, Long, ? extends O> enrich;

    AttachEventTimeProcessor(BiFunctionEx<? super I, Long, ? extends O> enrich) {
        this.enrich = enrich;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        if (item instanceof JetEvent) {
            JetEvent<I> jetEvent = (JetEvent<I>) item;
            I payload = jetEvent.payload();
            long timestamp = jetEvent.timestamp();
            O enrichedPayload = enrich.apply(payload, timestamp);
            item = JetEvent.jetEvent(timestamp, enrichedPayload);
        }
        return tryEmit(item);
    }
}
