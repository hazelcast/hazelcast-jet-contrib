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

import com.hazelcast.jet.contrib.connect.impl.AbstractKafkaConnectSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.source.SourceRecord;

import java.net.URL;
import java.util.Properties;

/**
 * Contains factory methods to create a Kafka Connect source.
 */
public final class KafkaConnectSources {

    private KafkaConnectSources() {
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka
     * Connect source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file
     * contains the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)}
     * respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the
     * configuration parameters as you'd using it with Kafka. Hazelcast
     * Jet will drive the Kafka Connect connector from the pipeline and
     * the records will be available to your pipeline as {@link SourceRecord}s.
     * <p>
     * In case of a failure; this source keeps track of the source
     * partition offsets, it will restore the partition offsets and
     * resume the consumption from where it left off.
     * <p>
     * Hazelcast Jet will instantiate a single task for the specified
     * source in the cluster.
     *
     * @param properties Kafka connect properties
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<SourceRecord> connect(Properties properties) {
        String name = properties.getProperty("name");
        return SourceBuilder.timestampedStream(name, ctx ->
                new KafkaConnectSource(properties))
                .fillBufferFn(KafkaConnectSource::fillBuffer)
                .createSnapshotFn(KafkaConnectSource::createSnapshot)
                .restoreSnapshotFn(KafkaConnectSource::restoreSnapshot)
                .destroyFn(KafkaConnectSource::destroy)
                .build();
    }

    private static class KafkaConnectSource extends AbstractKafkaConnectSource<SourceRecord> {

        KafkaConnectSource(Properties properties) {
            super(properties);
        }

        @Override
        protected boolean addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
            long ts = record.timestamp() == null ? 0 : record.timestamp();
            buf.add(record, ts);
            return true;
        }
    }
}
