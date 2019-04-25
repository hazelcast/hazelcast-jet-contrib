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

package com.hazelcast.jet.contrib.probabilistic;

import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogImpl;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;

/**
 * Factory class for probabilist aggregations.
 *
 */
public final class ProbabilisticAggregations {
    private static final int DEFAULT_HYPERLOGLOG_PRECISION = 14;
    private static final int LOWER_P_BOUND = 4;
    private static final int UPPER_P_BOUND = 16;

    private ProbabilisticAggregations() {

    }

    /**
     * Create an aggregation to estimate cardinality. It only accept <code>Long</code> as its input.
     * However you can use {@link HashingSupport} to map arbitrary object to 64 bits hashes.
     *
     * The implementation uses HyperLogLogPlus data structure. It has constant space complexity and configurable
     * precision.
     *
     * @return aggregation estimating cardinality.
     */
    public static AggregateOperation1<Long, ?, Long> hyperLogLog() {
        return hyperLogLog(DEFAULT_HYPERLOGLOG_PRECISION);
    }

    /**
     * Create an aggregation to estimate cardinality. It only accept <code>Long</code> as its input.
     * However you can use {@link HashingSupport} to map arbitrary object to 64 bits hashes.
     *
     * The implementation uses HyperLogLogPlus data structure. It has constant space complexity and configurable
     * precision.
     *
     * @param precision - HyperLogLog precision. It's in the range between 4 - 16. The higher precision reduced
     *                  estimation error at the expense of increased memory consumption.
     * @return aggregation estimating cardinality.
     */
    public static AggregateOperation1<Long, ?, Long> hyperLogLog(int precision) {
        if (precision < LOWER_P_BOUND || precision > UPPER_P_BOUND) {
            throw new IllegalArgumentException("Precision outside valid range [4..16].");
        }

        return AggregateOperation
                .withCreate(() -> new HyperLogLogImpl(precision))
                .andAccumulate(HyperLogLogImpl::add)
                .andCombine(HyperLogLogImpl::merge)
                .andExportFinish(HyperLogLogImpl::estimate);
    }
}
