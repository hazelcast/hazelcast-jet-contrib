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

package com.hazelcast.jet.contrib.deduplication;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;

/**
 * Module for stream elements deduplication.
 *
 */
public final class Deduplications {

    private Deduplications() {

    }

    /**
     * Deduplicate stream elements.
     *
     * Unlike {@link StageWithWindow#distinct()} or
     * {@link StageWithKeyAndWindow#distinct()} it emits distinct
     * elements as soon as they arrive without waiting for a window
     * to close. Thus it won't introduce latency to a stream processing
     * pipeline.
     *
     * It creates a window per each distinct element. Each window
     * has a configurable length.
     *
     * When a first element arrives it is immediately emitted to
     * downstream stages and a new window is created. When an equal
     * element arrives later and its matching window is still open
     * then this element is discarded and validity of the window
     * is extended.
     *
     * Deduplication window is driven by event-time hence the
     * window length is typically in milliseconds.
     *
     *
     * @param windowLength deduplication window length.
     * @param <T> type of elements in the stream
     * @return stream with duplicated elements discarded.
     */
    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> deduplicationWindow(long windowLength) {
        return deduplicationWindow(windowLength, FunctionEx.identity());
    }

    /**
     * Same as {@link #deduplicationWindow(long)} except it allows
     * to specify an extractor function. Each received element
     * is passed to this function and its output is used for
     * equality checks during duplicate detection.
     *
     * This is useful when you have a complex domain object
     * and you want to use only a specific field(s) for equality
     * checks.
     *
     * @param window deduplication window length.
     * @param extractor function to transform element before equality check
     * @param <T> type of elements in the stream
     * @return stream with duplicated elements discarded.
     */
    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> deduplicationWindow(long window,
                                                                                     FunctionEx<T, ?> extractor) {
        return stage -> stage.groupingKey(extractor)
                .filterStateful(window, () -> new boolean[1],
                    (s, i) -> {
                        boolean res = s[0];
                        s[0] = true;
                        return !res;
                    }
                );
    }
}
