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

package org.apache.inlong.sort.tchousex.flink.catalog;

import org.apache.inlong.sort.tchousex.flink.type.TCHouseXDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

    private TableIdentifier tableIdentifier;
    private List<Field> normalFields;
    private List<Field> partitionFields;
    private List<Field> primaryKeyFields;

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(TableIdentifier tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public List<Field> getNormalFields() {
        return normalFields;
    }

    public void setNormalFields(List<Field> normalFields) {
        this.normalFields = normalFields;
    }

    public List<Field> getPartitionFields() {
        return partitionFields;
    }

    public void setPartitionFields(List<Field> partitionFields) {
        this.partitionFields = partitionFields;
    }

    public List<Field> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public void setPrimaryKeyFields(List<Field> primaryKeyFields) {
        this.primaryKeyFields = primaryKeyFields;
    }

    public Table(TableIdentifier tableIdentifier,
            List<Field> normalFields, List<Field> partitionFields) {
        this.tableIdentifier = tableIdentifier;
        this.normalFields = normalFields;
        this.partitionFields = partitionFields;
    }

    public Table() {
        this.normalFields = new ArrayList<>();
        this.partitionFields = new ArrayList<>();
        this.primaryKeyFields = new ArrayList<>();
    }

    public Integer getFieldNum() {
        if (normalFields != null && partitionFields != null) {
            return normalFields.size() + partitionFields.size();
        }

        if (normalFields != null) {
            return normalFields.size();
        }
        throw new RuntimeException("table field list is null");
    }

    public List<Field> getAllFields() {
        List<Field> all = new ArrayList<>();
        if (normalFields != null) {
            all.addAll(normalFields);
        }
        if (partitionFields != null) {
            all.addAll(partitionFields);
        }
        return all;
    }

    public List<String> getAllFieldName() {
        List<String> all = new ArrayList<>();
        if (normalFields != null) {
            for (Field field : normalFields) {
                all.add(field.getName());
            }
        }
        if (partitionFields != null) {
            for (Field field : partitionFields) {
                all.add(field.getName());
            }
        }
        return all;
    }

    public Map<String, TCHouseXDataType> getFieldNameMapTCHouseXType() {
        Map<String, TCHouseXDataType> fieldMapType = new HashMap<>();
        for (Field field : this.getAllFields()) {
            fieldMapType.put(field.getName(), TCHouseXDataType.fromType(field.getType()));
        }
        return fieldMapType;
    }

    public Map<Integer, TCHouseXDataType> getFieldIndexMapTCHouseXType() {
        Map<Integer, TCHouseXDataType> fieldMapType = new HashMap<>();
        for (int i = 0; i < this.getAllFields().size(); i++) {
            fieldMapType.put(i, TCHouseXDataType.fromType(this.getAllFields().get(i).getType()));
        }
        return fieldMapType;
    }

    public List<Field> genPrimaryKeyFieldsByFieldName(List<String> fieldNames) {
        Map<String, String> fieldMapType = this.getFieldNameMapType();
        List<Field> primaryField = new ArrayList<>();
        for (String name : fieldNames) {
            primaryField.add(new Field(name, fieldMapType.get(name)));
        }
        return primaryField;
    }

    public Map<String, String> getFieldNameMapType() {
        Map<String, String> fieldMapType = new HashMap<>();
        for (Field field : this.getAllFields()) {
            fieldMapType.put(field.getName(), field.getType());
        }
        return fieldMapType;
    }

}
