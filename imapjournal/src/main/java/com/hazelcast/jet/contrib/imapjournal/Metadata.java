package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

public class Metadata {
    private final List<TableField> fields;
    private final QueryTargetDescriptor keyQueryTargetDescriptor;
    private final QueryTargetDescriptor valueQueryTargetDescriptor;

    public Metadata(
            List<TableField> fields,
            QueryTargetDescriptor keyQueryTargetDescriptor,
            QueryTargetDescriptor valueQueryTargetDescriptor
    ) {
        this.fields = fields;
        this.keyQueryTargetDescriptor = keyQueryTargetDescriptor;
        this.valueQueryTargetDescriptor = valueQueryTargetDescriptor;
    }

    public List<TableField> getFields() {
        return fields;
    }

    public QueryTargetDescriptor getKeyQueryTargetDescriptor() {
        return keyQueryTargetDescriptor;
    }

    public QueryTargetDescriptor getValueQueryTargetDescriptor() {
        return valueQueryTargetDescriptor;
    }
}
