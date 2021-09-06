package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class JournalQuerySerializableFactory implements DataSerializableFactory {

    public static final int FACTORY_ID = 8000;

    public static final int JOURNAL_QUERY_TARGET_DESCRIPTOR = 1;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == JOURNAL_QUERY_TARGET_DESCRIPTOR) {
            return new JournalQueryTargetDescriptor();
        } else {
            return null;
        }
    }
}
