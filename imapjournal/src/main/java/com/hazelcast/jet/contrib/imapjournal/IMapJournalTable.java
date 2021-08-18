package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;

public class IMapJournalTable extends JetTable implements Serializable {
    private JournalQueryTargetDescriptor queryTargetDescriptor;

    public IMapJournalTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull TableStatistics statistics,
            @Nonnull QueryTargetDescriptor keyQueryTargetDescriptor,
            @Nonnull QueryTargetDescriptor valueQueryTargetDescriptor
    ) {
        super(sqlConnector, fields, schemaName, name, statistics);
        this.queryTargetDescriptor = new JournalQueryTargetDescriptor(
                keyQueryTargetDescriptor,
                valueQueryTargetDescriptor
        );
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return null;
    }

    String[] paths() {
        return getFields().stream().map(field -> field.getName()).toArray(String[]::new);
    }

    QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    public JournalQueryTargetDescriptor getQueryTargetDescriptor() {
        return queryTargetDescriptor;
    }
}
