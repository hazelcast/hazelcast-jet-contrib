package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

public class JournalQueryTarget implements QueryTarget {
    private final QueryTarget keyTarget;
    private final QueryTarget oldValueTarget;
    private final QueryTarget newValueTarget;

    private Object[] targets;

    public JournalQueryTarget(
            QueryTarget keyTarget,
            QueryTarget oldValueTarget,
            QueryTarget newValueTarget
    ) {
        this.keyTarget = keyTarget;
        this.oldValueTarget = oldValueTarget;
        this.newValueTarget = newValueTarget;
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        targets = (Object[]) target;
        assert targets.length == 4;

        keyTarget.setTarget(targets[0], null);
        oldValueTarget.setTarget(targets[2], null);
        newValueTarget.setTarget(targets[3], null);
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path.startsWith("__key")) {
            return keyTarget.createExtractor(null, type);
            // TODO: Handle "__key.**"
        } else if (path.startsWith("old_")) {
            return oldValueTarget.createExtractor(path.replace("old_", ""), type);
        } else if (path.startsWith("old")) {
            return oldValueTarget.createExtractor(null, type);
        } else if (path.startsWith("new_")) {
            return newValueTarget.createExtractor(path.replace("new_", ""), type);
        } else if (path.startsWith("new")) {
            return newValueTarget.createExtractor(null, type);
        } else if (path.startsWith("type")) {
            return () -> type.convert(targets[1]);
        } else if (path.startsWith("this")) {
            // TODO: Remove this
            return () -> new Object();
        } else {
            throw new IllegalArgumentException("Unexpected path");
        }
    }
}
