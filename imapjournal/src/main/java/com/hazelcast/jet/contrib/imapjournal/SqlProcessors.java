package com.hazelcast.jet.contrib.imapjournal;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceP;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;

public class SqlProcessors {

    private SqlProcessors() {
    }

    public static ProcessorSupplier rowProjector(
            String[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor descriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        ServiceFactory<?, RowProjector> service =
                nonSharedService(ctx -> {
                    SimpleExpressionEvalContext evalContext = SimpleExpressionEvalContext.from(ctx);
                    QueryTarget target = descriptor.create(
                            evalContext.getSerializationService(),
                            Extractors.newBuilder(evalContext.getSerializationService()).build(),
                            false
                    );
                    return new RowProjector(paths, types, target, predicate, projection,
                            SimpleExpressionEvalContext.from(ctx));
                });
        return mapUsingServiceP(service, RowProjector::project);
    }
}
