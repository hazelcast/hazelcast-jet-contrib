package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class IMapJournalConnector implements SqlConnector {
    public static final String TYPE_NAME = "IMAP_JOURNAL";

    private static final IMapJournalMetadataResolvers METADATA_RESOLVERS = new IMapJournalMetadataResolvers(
            KvMetadataJavaResolver.INSTANCE
    );

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        assert table instanceof IMapJournalTable;

        IMapJournalTable t = (IMapJournalTable) table;

        Vertex v1 = dag.newUniqueVertex(
                "MapJournal",
                StreamEventJournalP.streamMapSupplier(
                        t.getSqlName(),
                        event -> true,
                        event -> new Object[]{ event.getKey(), event.getType(), event.getOldValue(), event.getNewValue() },
                        JournalInitialPosition.START_FROM_OLDEST,
                        EventTimePolicy.noEventTime()
                )
        );

        Vertex v2 = dag.newUniqueVertex(
                "ProjectMapJournal",
                SqlProcessors.rowProjector(
                        t.paths(),
                        t.types(),
                        t.getQueryTargetDescriptor(),
                        predicate,
                        projection
                )
        );

        dag.edge(Edge.between(v1, v2).isolated());
        return v2;
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        return METADATA_RESOLVERS.resolveAndValidateFields(
                userFields,
                options,
                (InternalSerializationService) nodeEngine.getSerializationService()
        );
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        Metadata metadata = METADATA_RESOLVERS.resolveMetadata(
                resolvedFields,
                options,
                (InternalSerializationService) nodeEngine.getSerializationService()
        );

        return new IMapJournalTable(
                this,
                metadata.getFields(),
                schemaName,
                externalName,
                new ConstantTableStatistics(0),
                metadata.getKeyQueryTargetDescriptor(),
                metadata.getValueQueryTargetDescriptor()
        );
    }
}
