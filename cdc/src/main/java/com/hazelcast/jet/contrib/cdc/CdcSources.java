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

package com.hazelcast.jet.contrib.cdc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.contrib.cdc.impl.ChangeEventMongoImpl;
import com.hazelcast.jet.contrib.cdc.impl.ChangeEventRelationalImpl;
import com.hazelcast.jet.contrib.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.data.Values;

import java.util.Properties;

/**
 * Contains factory methods for creating change data capture sources
 */
public final class CdcSources {

    //todo: update main README.md file in cdc module

    //todo: review all Debezium config options, see if we need to add more

    //todo: further refine and document config option

    //todo: should have one object mapper per CDC source instance
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private CdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from your MySQL
     * database to the Hazelcast Jet pipeline.
     *
     * @param name       ID of this particular CDC source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> mysql(String name, Properties properties) {
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        /*Flag that specifies if the connector should generate on the
        schema change topic named 'fulfillment' events with DDL changes
        that can be used by consumers.*/
        properties.putIfAbsent("include.schema.changes", "false");

        properties.putIfAbsent("tombstones.on.delete", "false");

        return KafkaConnectSources.connect(properties,
                record -> {
                    String keyJson = Values.convertToString(record.keySchema(), record.key());
                    String valueJson = Values.convertToString(record.valueSchema(), record.value());
                    return new ChangeEventRelationalImpl(keyJson, valueJson, OBJECT_MAPPER);
                }
        );
    }

    /**
     * Creates a CDC source that streams change data from your
     * PostgreSQL database to the Hazelcast Jet pipeline.
     *
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> postgres(String name, Properties properties) {
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        properties.putIfAbsent("tombstones.on.delete", "false");

        return KafkaConnectSources.connect(properties,
                record -> {
                    String keyJson = Values.convertToString(record.keySchema(), record.key());
                    String valueJson = Values.convertToString(record.valueSchema(), record.value());
                    return new ChangeEventRelationalImpl(keyJson, valueJson, OBJECT_MAPPER);
                }
        );
    }

    /**
     * Creates a CDC source that streams change data from your
     * Microsoft SQL Server database to the Hazelcast Jet pipeline.
     *
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> sqlserver(String name, Properties properties) {
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        properties.putIfAbsent("tombstones.on.delete", "false");

        return KafkaConnectSources.connect(properties,
                record -> {
                    String keyJson = Values.convertToString(record.keySchema(), record.key());
                    String valueJson = Values.convertToString(record.valueSchema(), record.value());
                    return new ChangeEventRelationalImpl(keyJson, valueJson, OBJECT_MAPPER);
                }
        );
    }

    /**
     * Creates a CDC source that streams change data from your
     * MongoDB database to the Hazelcast Jet pipeline.
     *
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> mongo(String name, Properties properties) {
        properties = copy(properties);

        /* Used internally as a unique identifier when recording the
        oplog position of each replica set. Needs to be set. */
        checkSet(properties, "mongodb.name");

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.mongodb.MongoDbConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        /* When running the connector against a sharded cluster, use a
        value of tasks.max that is greater than the number of replica
        sets. This will allow the connector to create one task for each
        replica set, and will let Kafka Connect coordinate, distribute,
        and manage the tasks across all of the available worker
        processes.*/
        properties.putIfAbsent("tasks.max", 1);

        /*Positive integer value that specifies the maximum number of
        threads used to perform an intial sync of the collections in a
        replica set.*/
        properties.putIfAbsent("initial.sync.max.threads", 1);

        properties.putIfAbsent("tombstones.on.delete", "false");

        return KafkaConnectSources.connect(properties,
                record -> {
                    String keyJson = Values.convertToString(record.keySchema(), record.key());
                    String valueJson = Values.convertToString(record.valueSchema(), record.value());
                    return new ChangeEventMongoImpl(keyJson, valueJson);
                }
        );
    }

    private static void checkSet(Properties properties, String key) {
        if (properties.get(key) == null) {
            throw new IllegalArgumentException("'" + key + "' should be set");
        }
    }

    private static Properties copy(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }


}
