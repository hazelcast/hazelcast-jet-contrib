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

package com.hazelcast.jet.contrib.influxdb.measurement;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import java.io.Serializable;
import java.time.Instant;

@Measurement(name = "cpu")
public class Cpu implements Serializable {
    @Column(name = "time")
    public Instant time;
    @Column(name = "host", tag = true)
    public String hostname;
    @Column(name = "load")
    public Double load;

    public Cpu() {
    }

    public Cpu(String hostname, Double load) {
        this.hostname = hostname;
        this.load = load;
    }

    @Override
    public String toString() {
        return "Cpu{" +
                "time=" + time +
                ", hostname='" + hostname + '\'' +
                ", load=" + load +
                '}';
    }
}
