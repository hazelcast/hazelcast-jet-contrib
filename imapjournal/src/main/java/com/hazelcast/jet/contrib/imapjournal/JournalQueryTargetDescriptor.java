package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;

import java.io.IOException;
import java.io.Serializable;

public class JournalQueryTargetDescriptor implements QueryTargetDescriptor, Serializable{
    private QueryTargetDescriptor keyTargetDescriptor;
    private QueryTargetDescriptor valueTargetDescriptor;

    public JournalQueryTargetDescriptor(
            QueryTargetDescriptor keyTargetDescriptor,
            QueryTargetDescriptor valueTargetDescriptor
    ) {
        this.keyTargetDescriptor = keyTargetDescriptor;
        this.valueTargetDescriptor = valueTargetDescriptor;
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new JournalQueryTarget(
                keyTargetDescriptor.create(serializationService, extractors, true),
                valueTargetDescriptor.create(serializationService, extractors, false),
                valueTargetDescriptor.create(serializationService, extractors, false));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(keyTargetDescriptor);
        out.writeObject(valueTargetDescriptor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyTargetDescriptor = in.readObject();
        valueTargetDescriptor = in.readObject();
    }
}
