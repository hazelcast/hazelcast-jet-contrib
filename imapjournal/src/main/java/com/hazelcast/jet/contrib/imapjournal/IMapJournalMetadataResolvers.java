package com.hazelcast.jet.contrib.imapjournal;

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class IMapJournalMetadataResolvers {
    private final Map<String, KvMetadataResolver> resolversMap;

    public IMapJournalMetadataResolvers(KvMetadataResolver... resolvers) {
        resolversMap = new HashMap<>();
        for (KvMetadataResolver resolver : resolvers) {
            resolver.supportedFormats().forEach(f -> resolversMap.put(f, resolver));
        }
    }

    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Stream<MappingField> keyMappings =
                findMetadataResolver(options, true).resolveAndValidateFields(
                        true,
                        userFields,
                        options,
                        serializationService
                );
        List<MappingField> typeMapping = singletonList(
                new MappingField("type", QueryDataType.VARCHAR, "type"));

        Stream<MappingField> valueMappings =
                findMetadataResolver(options, false).resolveAndValidateFields(
                        false,
                        userFields,
                        options,
                        serializationService
                );

        valueMappings = valueMappings.flatMap(m ->
                m.name().equals("this") ?
                        Stream.of(
                                new MappingField("old", m.type(), "old"),
                                new MappingField("new", m.type(), "new")
                        ) :
                        Stream.of(
                                new MappingField("old_" + m.name(), m.type(), "old_" + m.externalName()),
                                new MappingField("new_" + m.name(), m.type(), "new_" + m.externalName())
                        ));
        return Stream.concat(Stream.concat(keyMappings, typeMapping.stream()), valueMappings).collect(toList());
    }

    public Metadata resolveMetadata(
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        List<MappingField> resolvedFieldsNew = new ArrayList<>(resolvedFields);
        // Remove "type" and add "this", and check old and new exists
        resolvedFieldsNew.removeIf(e -> e.externalName().equals("type"));

        resolvedFieldsNew.add(new MappingField("this", getValueType(resolvedFieldsNew), "this"));

        KvMetadata keyMetadata =  findMetadataResolver(options, true)
                .resolveMetadata(true, resolvedFieldsNew, options, serializationService);
        KvMetadata valueMetadata = findMetadataResolver(options, false)
                .resolveMetadata(false, resolvedFieldsNew, options, serializationService);


        List<TableField> fields = Stream.concat(
                keyMetadata.getFields().stream(),
                Stream.concat(
                        Stream.of(new TableField("type", QueryDataType.VARCHAR, false)),
                        valueMetadata.getFields().stream().flatMap(e -> e.getName().equals("this")
                                        ? Stream.of(
                                new TableField("old", e.getType(), e.isHidden()),
                                new TableField("new", e.getType(), e.isHidden())
                                ) : Stream.of(new TableField(e.getName(), e.getType(), e.isHidden()))
                        )
                )).collect(toList());

        return new Metadata(
                fields,
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor()
        );
    }

    private QueryDataType getValueType(List<MappingField> resolvedFieldsNew) {
        QueryDataType dataTypeOfOld = null;
        QueryDataType dataTypeOfNew = null;

        for (MappingField mappingField : resolvedFieldsNew) {
            if (mappingField.name().equals("old")) {
                dataTypeOfOld = mappingField.type();
            }
            if (mappingField.name().equals("new")) {
                dataTypeOfNew = mappingField.type();
            }
        }

        if (dataTypeOfNew == null && dataTypeOfOld == null) {
            return QueryDataType.OBJECT;
        }

        assert dataTypeOfOld != null && dataTypeOfOld.equals(dataTypeOfNew);

        return dataTypeOfOld;
    }

    private KvMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error("Missing option: " + option);
        }

        KvMetadataResolver resolver = resolversMap.get(format);
        if (resolver == null) {
            throw QueryException.error("Unsupported serialization format: " + format);
        }
        return resolver;
    }
}
