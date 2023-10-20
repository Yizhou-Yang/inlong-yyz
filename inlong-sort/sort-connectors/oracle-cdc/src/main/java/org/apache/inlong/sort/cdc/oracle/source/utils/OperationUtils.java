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

package org.apache.inlong.sort.cdc.oracle.source.utils;

import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.inlong.sort.cdc.base.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.enums.AlterType;
import org.apache.inlong.sort.protocol.ddl.enums.IndexType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.indexes.Index;
import org.apache.inlong.sort.protocol.ddl.operations.AlterOperation;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.ddl.operations.DropTableOperation;
import org.apache.inlong.sort.protocol.ddl.operations.Operation;
import org.apache.inlong.sort.protocol.ddl.operations.RenameTableOperation;
import org.apache.inlong.sort.protocol.ddl.operations.TruncateTableOperation;
import org.apache.inlong.sort.protocol.ddl.operations.UnsupportedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.inlong.sort.protocol.ddl.Utils.ColumnUtils.parseColumnWithPosition;
import static org.apache.inlong.sort.protocol.ddl.Utils.ColumnUtils.parseColumns;
import static org.apache.inlong.sort.protocol.ddl.Utils.ColumnUtils.parseComment;
import static org.apache.inlong.sort.protocol.ddl.Utils.ColumnUtils.reformatName;

/**
 * Utils for generate operation from statement from sqlParser.
 */
public class OperationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataDebeziumDeserializeSchema.class);
    public static final String PRIMARY_KEY = "PRIMARY KEY";
    public static final String NORMAL_INDEX = "NORMAL_INDEX";
    public static final String FIRST = "FIRST";
    public static final List<Pattern> ESCAPE_PATTERNS =
            Arrays.asList(Pattern.compile("([\\s]*DROP[\\s]+TABLE[\\s]+.*)([\\s]+AS[\\s]+.*)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL));

    /**
     * generate operation from sql and table schema.
     * @param sql sql from binlog
     * @param tableSchema table schema
     * @return Operation
     */
    public static Operation generateOperation(String sql, TableChange tableSchema) {
        try {
            // now sqlParser don't support FIRST position
            // remove it first and add it later
            boolean endsWithFirst = sql.endsWith(FIRST);
            if (endsWithFirst) {
                sql = removeFirstFlag(sql);
            }
            sql = escape(sql);
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                return parseAlterOperation(
                        (Alter) statement, tableSchema, endsWithFirst);
            } else if (statement instanceof CreateTable) {
                return parseCreateTableOperation(
                        (CreateTable) statement, tableSchema);
            } else if (statement instanceof Drop) {
                return new DropTableOperation();
            } else if (statement instanceof Truncate) {
                return new TruncateTableOperation();
            } else if (statement instanceof RenameTableStatement) {
                return new RenameTableOperation();
            } else {
                LOG.error("doesn't support sql {}, statement {}", sql, statement);
            }
        } catch (Exception e) {
            LOG.error("parse ddl in sql {} error", sql, e);
        }
        return new UnsupportedOperation();
    }

    public static String escape(String sql) {
        for (Pattern pattern : ESCAPE_PATTERNS) {
            Matcher matcher = pattern.matcher(sql);
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        return sql;
    }

    /**
     * parse alter operation from Alter from sqlParser.
     * @param statement alter statement
     * @param tableSchema table schema
     * @param isFirst whether the column is first
     * @return AlterOperation
     */
    private static AlterOperation parseAlterOperation(Alter statement,
            TableChange tableSchema, boolean isFirst) {

        Map<String, Integer> sqlType = getSqlType(tableSchema);
        List<AlterColumn> alterColumns = new ArrayList<>();
        statement.getAlterExpressions().forEach(alterExpression -> {
            switch (alterExpression.getOperation()) {
                case DROP:
                    if (alterExpression.getIndex() != null) {
                        LOG.error("unsupported statement {}", statement);
                        throw new IllegalStateException("drop index not supported now");
                    }
                    if (alterExpression.getConstraintName() != null) {
                        alterColumns.add(new AlterColumn(AlterType.DROP_CONSTRAINT,
                                new Column(reformatName(alterExpression.getConstraintName())),
                                null));
                        break;
                    }
                    alterColumns.add(new AlterColumn(AlterType.DROP_COLUMN,
                            null,
                            Column.builder().name(reformatName(alterExpression.getColumnName()))
                                    .build()));
                    break;
                case ADD:
                    if (alterExpression.getIndex() != null) {
                        alterColumns.add(new AlterColumn(AlterType.ADD_CONSTRAINT,
                                null,
                                null));
                        break;
                    }
                    alterColumns.add(new AlterColumn(AlterType.ADD_COLUMN,
                            parseColumnWithPosition(isFirst, sqlType,
                                    alterExpression.getColDataTypeList().get(0)),
                            null));
                    break;
                case RENAME:
                    alterColumns.add(new AlterColumn(AlterType.CHANGE_COLUMN,
                            new Column(reformatName(alterExpression.getColumnName())),
                            new Column(reformatName(alterExpression.getColumnOldName()))));
                    break;
                case MODIFY:
                case CHANGE:
                    alterColumns.add(new AlterColumn(AlterType.CHANGE_COLUMN,
                            parseColumnWithPosition(isFirst, sqlType,
                                    alterExpression.getColDataTypeList().get(0)),
                            new Column(reformatName(alterExpression.getColumnOldName()))));
                    break;
                default:
                    LOG.warn("doesn't support alter operation {}, statement {}",
                            alterExpression.getOperation(), statement);
                    throw new IllegalStateException("statement not supported now");
            }

        });

        return new AlterOperation(alterColumns);
    }

    /**
     * parse create table operation from CreateTable from sqlParser.
     * @param statement create table statement
     * @param tableSchema table schema
     * @return CreateTableOperation
     */
    private static CreateTableOperation parseCreateTableOperation(
            CreateTable statement, TableChange tableSchema) {

        Map<String, Integer> sqlType = getSqlType(tableSchema);
        CreateTableOperation createTableOperation = new CreateTableOperation();
        List<ColumnDefinition> columnDefinitions = statement.getColumnDefinitions();

        if (statement.getLikeTable() != null) {
            createTableOperation.setLikeTable(parseLikeTable(statement));
            return createTableOperation;
        }

        createTableOperation.setColumns(parseColumns(sqlType, columnDefinitions));
        createTableOperation.setIndexes(parseIndexes(statement));
        createTableOperation.setComment(parseComment(statement.getTableOptionsStrings()));
        return createTableOperation;
    }

    /**
     * parse indexes from statement
     * only support primary key and normal index.
     * @param statement create table statement
     * @return list of indexes
     */
    private static List<Index> parseIndexes(CreateTable statement) {

        if (statement.getIndexes() == null) {
            return new ArrayList<>();
        }
        List<Index> indexList = new ArrayList<>();

        for (net.sf.jsqlparser.statement.create.table.Index perIndex : statement.getIndexes()) {
            Index index = new Index();
            switch (perIndex.getType()) {
                case PRIMARY_KEY:
                    index.setIndexType(IndexType.PRIMARY_KEY);
                    break;
                case NORMAL_INDEX:
                    index.setIndexType(IndexType.NORMAL_INDEX);
                    break;
                default:
                    LOG.error("unsupported index type {}", perIndex.getType());
                    break;
            }
            List<String> columns = new ArrayList<>();
            perIndex.getColumnsNames().forEach(columnName -> columns.add(reformatName(columnName)));
            index.setIndexName(reformatName(perIndex.getName()));
            index.setIndexColumns(columns);
            indexList.add(index);
        }

        return indexList;

    }

    /**
     * remove the first flag from sql.
     * @param sql sql from binlog
     * @return sql without first flag
     */
    private static String removeFirstFlag(String sql) {
        return sql.substring(0, sql.lastIndexOf(FIRST));
    }

    /**
     * get like table from statement.
     * @param statement create table statement
     * @return like table name
     */
    private static String parseLikeTable(CreateTable statement) {
        if (statement.getLikeTable() != null) {
            return statement.getLikeTable().getName();
        }
        return "";
    }

    public static Map<String, Integer> getSqlType(@Nullable TableChanges.TableChange tableSchema) {
        if (tableSchema == null) {
            return null;
        }
        Map<String, Integer> sqlType = new LinkedHashMap<>();
        final Table table = tableSchema.getTable();
        table.columns()
                .forEach(
                        column -> {
                            if (column.jdbcType() == Types.NUMERIC
                                    && tableSchema.getTable().isPrimaryKeyColumn(column.name())
                                    && column.scale().orElse(0) == 0) {
                                // Convert number(0) to bigint for Oracle when the column is a primary key
                                sqlType.put(
                                        column.name(),
                                        Types.BIGINT);
                            } else {
                                sqlType.put(
                                        column.name(),
                                        column.jdbcType());
                            }
                        });
        return sqlType;
    }
}
