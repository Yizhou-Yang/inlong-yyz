/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.util;

import com.google.common.base.Preconditions;
import org.apache.inlong.sort.protocol.ddl.enums.AlterType;
import org.apache.inlong.sort.protocol.ddl.operations.AlterOperation;
import org.apache.inlong.sort.protocol.ddl.operations.Operation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * Schema-change Utils
 */
public final class SchemaChangeUtils {

    private final static String DELIMITER = "&";
    private final static String KEY_VALUE_DELIMITER = "=";

    private SchemaChangeUtils() {
    }

    /**
     * deserialize the policies to a Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     *
     * @param policies The policies format by 'key1=value1&key2=value2...'
     * @return A policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     */
    public static Map<SchemaChangeType, SchemaChangePolicy> deserialize(String policies) {
        Preconditions.checkNotNull(policies, "policies is null");
        Map<SchemaChangeType, SchemaChangePolicy> policyMap = new HashMap<>();
        for (String kv : policies.split(DELIMITER)) {
            int index = kv.indexOf(KEY_VALUE_DELIMITER);
            if (index < 1 || index == kv.length() - 1) {
                throw new IllegalArgumentException(
                        "The format of policies must be like 'key1=value1&key2=value2...'");
            }
            String typeCode = kv.substring(0, index);
            String policyCode = kv.substring(index + 1);
            SchemaChangeType type;
            SchemaChangePolicy policy;
            try {
                type = SchemaChangeType.getInstance(Integer.parseInt(typeCode));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Unsupported type of schema-change: %s for InLong", typeCode));
            }
            try {
                policy = SchemaChangePolicy.getInstance(Integer.parseInt(policyCode));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Unsupported policy of schema-change: %s for InLong", policyCode));
            }
            policyMap.put(type, policy);
        }
        return policyMap;
    }

    /**
     * Serialize the policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}] to a string
     *
     * @param policyMap The policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     * @return A string format by 'key1=value1&key2=value2...'
     */
    public static String serialize(Map<SchemaChangeType, SchemaChangePolicy> policyMap) {
        Preconditions.checkNotNull(policyMap, "policyMap is null");
        StringJoiner joiner = new StringJoiner("&");
        for (Entry<SchemaChangeType, SchemaChangePolicy> kv : policyMap.entrySet()) {
            joiner.add(kv.getKey().getCode() + "=" + kv.getValue().getCode());
        }
        return joiner.toString();
    }

    /**
     * Extract the schema change type from {@link Operation}
     *
     * @param operation The operation
     * @return A type of {@link SchemaChangeType}
     */
    public static SchemaChangeType extractSchemaChangeType(Operation operation) {
        SchemaChangeType type = null;
        switch (operation.getOperationType()) {
            case ALTER:
                AlterOperation alterOperation = (AlterOperation) operation;
                // Get alter type from the first item
                Preconditions.checkState(alterOperation.getAlterColumns() != null
                        && !alterOperation.getAlterColumns().isEmpty(), "alter columns is empty");
                AlterType alterType = alterOperation.getAlterColumns().get(0).getAlterType();
                switch (alterType) {
                    case ADD_COLUMN:
                        type = SchemaChangeType.ADD_COLUMN;
                        break;
                    case DROP_COLUMN:
                        type = SchemaChangeType.DROP_COLUMN;
                        break;
                    case RENAME_COLUMN:
                        type = SchemaChangeType.RENAME_COLUMN;
                        break;
                    default:
                }
                break;
            case CREATE:
                type = SchemaChangeType.CREATE_TABLE;
                break;
            case TRUNCATE:
                type = SchemaChangeType.TRUNCATE_TABLE;
                break;
            case RENAME:
                type = SchemaChangeType.RENAME_TABLE;
                break;
            case DROP:
                type = SchemaChangeType.DROP_TABLE;
            default:
        }
        return type;
    }
}
