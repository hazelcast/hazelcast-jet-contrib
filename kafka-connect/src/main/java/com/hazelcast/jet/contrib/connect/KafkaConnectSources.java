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

package com.hazelcast.jet.contrib.connect;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import javax.annotation.Nonnull;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Contains factory methods to create a Kafka Connect source.
 */
public final class KafkaConnectSources {

    private KafkaConnectSources() {
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka Connect
     * source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file contains
     * the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)} respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the configuration
     * parameters as you'd using it with Kafka. Hazelcast Jet will drive the
     * Kafka Connect connector from the pipeline and the records will be available
     * to your pipeline as {@link SourceRecord}s.
     * <p>
     * In case of a failure; this source keeps track of the source partition
     * offsets, it will restore the partition offsets and resume the consumption
     * from where it left off.
     * <p>
     * Hazelcast Jet will instantiate a single task for the specified source in
     * the cluster.
     *
     * @param properties Kafka connect properties
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<SourceRecord> connect(Properties properties) {
        return connect(properties, FunctionEx.identity());
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka Connect
     * source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file contains
     * the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)} respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the configuration
     * parameters as you'd using it with Kafka. Hazelcast Jet will drive the
     * Kafka Connect connector from the pipeline and the records will be available
     * to your pipeline as {@link SourceRecord}s.
     * <p>
     * In case of a failure; this source keeps track of the source partition
     * offsets, it will restore the partition offsets and resume the consumption
     * from where it left off.
     * <p>
     * Hazelcast Jet will instantiate a single task for the specified source in
     * the cluster.
     *
     * @param properties   Kafka connect properties
     * @param projectionFn mapping from {@code SourceRecord} to whatever type we need the source
     *                     to return
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static <T> StreamSource<T> connect(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super SourceRecord, ? extends T> projectionFn) {
        String name = properties.getProperty("name");
        return SourceBuilder.timestampedStream(name, ctx -> new Context<T>(ctx, properties, projectionFn))
                .fillBufferFn(Context<T>::fillBuffer)
                .createSnapshotFn(Context::createSnapshot)
                .restoreSnapshotFn(Context::restoreSnapshot)
                .destroyFn(Context::destroy)
                .build();
    }

    private static class Context<T> {

        private final SourceConnector connector;
        private final SourceTask task;
        private final Map<String, String> taskConfig;
        private final FunctionEx<? super SourceRecord, ? extends T> projectionFn;

        /**
         * Key represents the partition which the record originated from. Value
         * represents the offset within that partition. Kafka Connect represents
         * the partition and offset as arbitrary values so that is why it is
         * stored as map.
         * See {@link SourceRecord} for more information regarding the format.
         */
        private Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
        private boolean taskInit;

        Context(Processor.Context ctx, Properties properties, FunctionEx<? super SourceRecord, ? extends T> projectionFn) {
            try {
                if (isDebezium(properties)) {
                    // inject hazelcast.instance.name for retrieving from JVM instance factory in the Debezium source
                    injectHazelcastInstanceNameProperty(ctx, properties);
                }
                String connectorClazz = properties.getProperty("connector.class");
                Class<?> connectorClass = Thread.currentThread().getContextClassLoader().loadClass(connectorClazz);
                connector = (SourceConnector) connectorClass.getConstructor().newInstance();
                connector.initialize(new JetConnectorContext());
                connector.start((Map) properties);

                taskConfig = connector.taskConfigs(1).get(0);
                task = (SourceTask) connector.taskClass().getConstructor().newInstance();

                this.projectionFn = projectionFn;
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        private void injectHazelcastInstanceNameProperty(Processor.Context ctx, Properties properties) {
            JetInstance jet = ctx.jetInstance();
            String instanceName = HazelcastInstanceFactory.getInstanceName(jet.getName(),
                    jet.getHazelcastInstance().getConfig());
            properties.setProperty("database.history.hazelcast.instance.name", instanceName);
        }

        private boolean isDebezium(Properties properties) {
            return properties.containsKey("database.history");
        }

        void fillBuffer(TimestampedSourceBuffer<T> buf) {
            if (!taskInit) {
                task.initialize(new JetSourceTaskContext());
                task.start(taskConfig);
                taskInit = true;
            }
            try {
                List<SourceRecord> records = task.poll();
                if (records == null) {
                    return;
                }

                for (SourceRecord record : records) {
                    T projection = projectionFn.apply(record);
                    if (projection != null) {
                        long ts = record.timestamp() == null ? 0 : record.timestamp();
                        buf.add(projection, ts);
                        partitionsToOffset.put(record.sourcePartition(), record.sourceOffset());
                    }
                }
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
        }

        void destroy() {
            try {
                task.stop();
            } finally {
                connector.stop();
            }
        }

        Map<Map<String, ?>, Map<String, ?>> createSnapshot() {
            return partitionsToOffset;
        }

        void restoreSnapshot(List<Map<Map<String, ?>, Map<String, ?>>> snapshots) {
            this.partitionsToOffset = snapshots.get(0);
        }

        private static class JetConnectorContext implements ConnectorContext {
            @Override
            public void requestTaskReconfiguration() {
                // no-op since it is not supported
            }

            @Override
            public void raiseError(Exception e) {
                rethrow(e);
            }
        }

        private class SourceOffsetStorageReader implements OffsetStorageReader {
            @Override
            public <V> Map<String, Object> offset(Map<String, V> partition) {
                return offsets(Collections.singletonList(partition)).get(partition);
            }

            @Override
            public <V> Map<Map<String, V>, Map<String, Object>> offsets(Collection<Map<String, V>> partitions) {
                Map<Map<String, V>, Map<String, Object>> map = new HashMap<>();
                for (Map<String, V> partition : partitions) {
                    Map<String, Object> offset = (Map<String, Object>) partitionsToOffset.get(partition);
                    map.put(partition, offset);
                }
                return map;
            }
        }

        private class JetSourceTaskContext implements SourceTaskContext {
            @Override
            public Map<String, String> configs() {
                return taskConfig;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new SourceOffsetStorageReader();
            }
        }
    }

}
