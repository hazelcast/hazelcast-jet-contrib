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

package com.hazelcast.jet.contrib.debezium;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.impl.ChangeEventMongoImpl;
import com.hazelcast.jet.cdc.impl.ChangeEventRelationalImpl;
import com.hazelcast.jet.contrib.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.StreamSource;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Values;

import java.util.Properties;

/**
 * Contains factory methods for creating Debezium connector sources
 */
public final class DebeziumSources {

    //todo: should have one object mapper per CDC source instance
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    //todo: should be independent CDCSources, no debezium exposed

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
    public static StreamSource<ChangeEvent> cdc(Configuration configuration) {
        //todo: configuration shouldn't be debezium based either
        Properties properties = configuration.edit()
                                             .with("database.history", HazelcastListDatabaseHistory.class.getName())
                                             .build()
                                             .asProperties();
        boolean mongodb = isMongoDB(properties);
        return KafkaConnectSources.connect(properties,
                record -> {
                    String keyJson = Values.convertToString(record.keySchema(), record.key());
                    String valueJson = Values.convertToString(record.valueSchema(), record.value());
                    return mongodb ? new ChangeEventMongoImpl(keyJson, valueJson) :
                            new ChangeEventRelationalImpl(keyJson, valueJson, OBJECT_MAPPER);
                }
        );
    }

    private static boolean isMongoDB(Properties properties) { //todo: ugly hack, but lacking alternatives...
        return properties.getProperty("connector.class").toLowerCase().contains("mongodb");
    }


}
