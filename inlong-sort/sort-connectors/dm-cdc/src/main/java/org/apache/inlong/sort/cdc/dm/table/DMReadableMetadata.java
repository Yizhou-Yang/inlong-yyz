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

package org.apache.inlong.sort.cdc.dm.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.sql.Timestamp;

public enum DMReadableMetadata {

    /** Name of the database that contains the row. */
    DATABASE(
            "meta.database_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getDatabase());
                }
            }),

    /** Name of the database that contains the row. */
    SCHEMA(
            "meta.schema_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getSchema());
                }
            }),

    /** Name of the table that contains the row. */
    TABLE(
            "meta.table_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getTable());
                }
            }),

    /**
     * It indicates the scn that the change was made in the database. If the record is read from
     * snapshot of the table instead of the change stream, the value is always 0.
     */
    SCN(
            "scn",
            DataTypes.BIGINT().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return record.getSourceInfo().getSCN();
                }
            }),

    /**
     * It indicates the type of the operation
     */
    OP_TYPE(
            "meta.event_type",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getOpt().toString());
                }
            }),

    /**
     * It indicates the time when the data is written to the database
     * TODO: support this metadata
     */
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP_LTZ().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    long timestampmillis = record.getTimeProcessed();
                    Timestamp timestamp = new Timestamp(timestampmillis);
                    timestamp.setNanos(0); // set nanoseconds to 0 to match the precision of TIMESTAMP_LTZ
                    return TimestampData.fromTimestamp(timestamp);
                }
            });

    private final String key;

    private final DataType dataType;

    private final DMMetadataConverter converter;

    DMReadableMetadata(String key, DataType dataType, DMMetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public DMMetadataConverter getConverter() {
        return converter;
    }
}
