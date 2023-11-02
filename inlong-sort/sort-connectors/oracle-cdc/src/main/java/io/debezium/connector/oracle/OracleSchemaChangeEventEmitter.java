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

package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.logminer.processor.TruncateReceiver;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.relational.ddl.DdlParserListener.TableAlteredEvent;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableDroppedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableTruncatedEvent;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle
 * Copy from debezium-connector-oracle 1.9.7.Final
 *
 * @author Gunnar Morling
 */
public class OracleSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSchemaChangeEventEmitter.class);

    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final TableId tableId;
    private final OracleDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final TruncateReceiver truncateReceiver;

    public OracleSchemaChangeEventEmitter(OracleConnectorConfig connectorConfig, OraclePartition partition,
            OracleOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
            String objectOwner, String ddlText, OracleDatabaseSchema schema,
            Instant changeTime, OracleStreamingChangeEventSourceMetrics streamingMetrics,
            TruncateReceiver truncateReceiver) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.schema = schema;
        this.changeTime = changeTime;
        this.streamingMetrics = streamingMetrics;
        this.filters = connectorConfig.getTableFilters().dataCollectionFilter();
        this.truncateReceiver = truncateReceiver;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        // Cache a copy of the table's schema prior to parsing the DDL.
        // This is needed in the event that the parsed DDL is a drop table
        // todo: verify whether this is actually necessary in the emitted SchemaChangeEvent
        final Table tableBefore = schema.tableFor(tableId);
        final OracleDdlParser parser = schema.getDdlParser();
        final DdlChanges ddlChanges = parser.getDdlChanges();
        try {
            ddlChanges.reset();
            parser.setCurrentDatabase(sourceDatabaseName);
            parser.setCurrentSchema(objectOwner);
            parser.parse(ddlText, schema.getTables());
        } catch (ParsingException | MultipleParsingExceptions e) {
            if (schema.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}': {}", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementUnparsableDdlCount();
            } else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty() && (filters.isIncluded(tableId) || !schema.storeOnlyCapturedTables())) {
            List<SchemaChangeEvent> changeEvents = new ArrayList<>();
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    switch (event.type()) {
                        case CREATE_TABLE:
                            changeEvents.add(createTableEvent(partition, (TableCreatedEvent) event));
                            break;
                        case ALTER_TABLE:
                            changeEvents.add(alterTableEvent(partition, (TableAlteredEvent) event));
                            break;
                        case DROP_TABLE:
                            changeEvents.add(dropTableEvent(partition, tableBefore, (TableDroppedEvent) event));
                            break;
                        case TRUNCATE_TABLE:
                            changeEvents.add(truncateTableEvent(partition, tableBefore, (TableTruncatedEvent) event));
                            break;
                        default:
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlText);
                            break;
                    }
                });
            });

            for (SchemaChangeEvent event : changeEvents) {
                receiver.schemaChangeEvent(event);
            }
        }
    }

    private SchemaChangeEvent createTableEvent(OraclePartition partition, TableCreatedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                false);
    }

    private SchemaChangeEvent alterTableEvent(OraclePartition partition, TableAlteredEvent event) {
        final Set<TableId> tableIds = new LinkedHashSet<>();
        tableIds.add(tableId);
        tableIds.add(event.tableId());

        offsetContext.tableEvent(tableIds, changeTime);
        if (tableId == null) {
            return SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    null,
                    null,
                    event.statement(),
                    schema.tableFor(event.tableId()));
        } else {
            return SchemaChangeEvent.ofRename(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()),
                    tableId);
        }
    }

    private SchemaChangeEvent truncateTableEvent(OraclePartition partition,
            Table tableSchema, TableTruncatedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofDatabase(
                partition,
                offsetContext,
                tableId.catalog(),
                event.statement(),
                false);
    }

    private SchemaChangeEvent dropTableEvent(OraclePartition partition, Table tableSchemaBeforeDrop,
            TableDroppedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofDrop(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                tableSchemaBeforeDrop);
    }
}
