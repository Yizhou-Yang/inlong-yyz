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

package org.apache.inlong.sort.tchousex.flink.table;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.tchousex.flink.function.GenericTCHouseXSinkFunction;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions;

import javax.annotation.Nullable;
import java.util.List;

public class TCHouseXDynamicTableSink implements DynamicTableSink {

    private final TableSchema tableSchema;
    private final boolean multipleSink;
    private final String sinkMultipleFormat;
    private final String databasePattern;
    private final String tablePattern;
    private final SchemaUpdateExceptionPolicy schemaUpdatePolicy;
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private final Integer parallelism;
    private final DirtyOptions dirtyOptions;
    @Nullable
    private final DirtySink<Object> dirtySink;
    private final boolean enableSchemaChange;
    @Nullable
    private final String schemaChangePolicies;
    private final TCHouseXSinkOptions tcHouseXSinkOptions;
    private final TCHouseXOptions tcHouseXOptions;
    private final String uid;

    public TCHouseXDynamicTableSink(
            TCHouseXOptions tcHouseXOptions,
            TCHouseXSinkOptions tcHouseXSinkOptions,
            TableSchema tableSchema,
            boolean multipleSink,
            String sinkMultipleFormat,
            String databasePattern,
            String tablePattern,
            SchemaUpdateExceptionPolicy schemaUpdatePolicy,
            String inlongMetric,
            String auditHostAndPorts,
            Integer parallelism,
            DirtyOptions dirtyOptions,
            @Nullable DirtySink<Object> dirtySink,
            boolean enableSchemaChange,
            @Nullable String schemaChangePolicies,
            @Nullable String uid) {
        this.tcHouseXOptions = tcHouseXOptions;
        this.tcHouseXSinkOptions = tcHouseXSinkOptions;
        this.tableSchema = tableSchema;
        this.multipleSink = multipleSink;
        this.sinkMultipleFormat = sinkMultipleFormat;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.schemaUpdatePolicy = schemaUpdatePolicy;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.parallelism = parallelism;
        this.dirtyOptions = dirtyOptions;
        this.dirtySink = dirtySink;
        this.enableSchemaChange = enableSchemaChange;
        this.schemaChangePolicies = schemaChangePolicies;
        this.uid = uid;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.all();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        List<String> primaryFields = null;
        if (!multipleSink) {
            primaryFields = tableSchema.getPrimaryKey().get().getColumns();
        }
        TCHouseXDynamicOutputFormat.Builder builder = TCHouseXDynamicOutputFormat.builder()
                .setTCHouseXOptions(tcHouseXOptions)
                .setTCHouseXSinkOptions(tcHouseXSinkOptions)
                .setInlongMetric(inlongMetric)
                .setAuditHostAndPorts(auditHostAndPorts)
                .setFieldDataTypes(tableSchema.getFieldDataTypes())
                .setFieldNames(tableSchema.getFieldNames())
                .setPrimaryFields(primaryFields)
                .setInlongMetric(inlongMetric)
                .setAuditHostAndPorts(auditHostAndPorts)
                .setMultipleSink(multipleSink)
                .setDatabasePattern(databasePattern)
                .setTablePattern(tablePattern)
                .setDynamicSchemaFormat(sinkMultipleFormat)
                .setSchemaUpdatePolicy(schemaUpdatePolicy)
                .setDirtyOptions(dirtyOptions)
                .setDirtySink(dirtySink)
                .setEnableSchemaChange(enableSchemaChange)
                .setSchemaChangePolicies(schemaChangePolicies);
        return (DataStreamSinkProvider) dataStream -> {
            DataStreamSink<RowData> sink =
                    dataStream.addSink(new GenericTCHouseXSinkFunction<>(builder.build()));

            if (parallelism != null) {
                sink = sink.setParallelism(parallelism);
            }
            if (uid != null) {
                sink = sink.uid(uid);
            }
            return sink;
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new TCHouseXDynamicTableSink(
                tcHouseXOptions,
                tcHouseXSinkOptions,
                tableSchema,
                multipleSink,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                schemaUpdatePolicy,
                inlongMetric,
                auditHostAndPorts,
                parallelism,
                dirtyOptions,
                dirtySink,
                enableSchemaChange,
                schemaChangePolicies,
                uid);
    }

    @Override
    public String asSummaryString() {
        return "TCHouse-X Table Sink Of InLong";
    }
}
