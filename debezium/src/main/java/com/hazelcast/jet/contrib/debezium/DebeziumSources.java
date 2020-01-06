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

package com.hazelcast.jet.contrib.debezium;

import com.hazelcast.jet.contrib.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.StreamSource;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

/**
 * Contains factory methods for creating Debezium connector sources
 */
public final class DebeziumSources {

    private DebeziumSources() {
    }

    /**
     * Creates a Debezium source that streams change data from
     * your databases to the Hazelcast Jet pipeline.
     *
     * @param configuration configuration object which holds the configuration
     *                      properties of the Debezium connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<SourceRecord> cdc(Configuration configuration) {
        Properties properties = configuration.edit()
                                             .with("database.history", HazelcastListDatabaseHistory.class.getName())
                                             .build()
                                             .asProperties();
        return KafkaConnectSources.connect(properties);
    }


}
