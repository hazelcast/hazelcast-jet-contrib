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


package mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A mapper which maps a single measurement row to an user object.
 *
 * @param <R> returned user object type.
 */
@FunctionalInterface
public interface MeasurementMapper<R> extends Serializable {

    /**
     * @param name        measurement name
     * @param tags        tag set for the measurement
     * @param columnNames list of column names
     * @param values      list of values
     * @return <R> user object.
     */
    R apply(String name, Map<String, String> tags, List<String> columnNames, List<Object> values);

}
