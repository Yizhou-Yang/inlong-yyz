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

package org.apache.inlong.sort.jdbc.dialect;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link PostgresDialect}
 */
public class PostgresDialectTest {

    private PostgresDialect dialect;

    @Before
    public void init() {
        dialect = new PostgresDialect();
    }

    @Test
    public void testParseUnknownDatabase() {
        Assert.assertTrue(dialect.parseUnknownDatabase(new SQLException("database not exists", "3D000")));
    }

    @Test
    public void testParseUnkownTable() {
        Assert.assertTrue(dialect.parseUnkownTable(new SQLException("table not exists", "42P01")));

    }

    @Test
    public void testParseUnkownSchema() {
        Assert.assertTrue(dialect.parseUnkownSchema(new SQLException("schema not exists", "3F000")));
    }

    @Test
    public void testParseResourceExistsError() {
        List<SQLException> resourceNotExistsList = Arrays
                .asList(new SQLException("", "42P04"), new SQLException("", "42P06"),
                        new SQLException("", "42P07"), new SQLException("", "42701"));
        for (SQLException e : resourceNotExistsList) {
            Assert.assertTrue(dialect.parseResourceExistsError(e));
        }
    }

    @Test
    public void testBuildCreateTableStatement() {
        String tableIdentifier = "test_database.test_schema.test_table";
        List<String> primaryKeys = Arrays.asList("class", "name");
        String comment = "test build create table sql";
        List<RowField> fields = Arrays.asList(
                new RowField("class", new VarCharType(10), "class"),
                new RowField("name", new VarCharType(11), "name"),
                new RowField("age", new IntType(), "age"),
                new RowField("money", new DecimalType(11, 3), "money"),
                new RowField("height", new DoubleType(), "height"),
                new RowField("weight", new FloatType(), "weight"));
        RowType rowType = new RowType(true, fields);
        String stmt = dialect.buildCreateTableStatement(tableIdentifier, primaryKeys, rowType, comment);
        String expected = "CREATE TABLE IF NOT EXISTS `test_database`.`test_schema`.`test_table` (\n"
                + "\t`class` VARCHAR(10) COMMENT 'class',\n"
                + "\t`name` VARCHAR(11) COMMENT 'name',\n"
                + "\t`age` INTEGER COMMENT 'age',\n"
                + "\t`money` DECIMAL(11, 3) COMMENT 'money',\n"
                + "\t`height` DOUBLE PRECISION COMMENT 'height',\n"
                + "\t`weight` REAL COMMENT 'weight',\n"
                + "\t PRIMARY KEY (`class`,`name`)\n"
                + ") COMMENT 'test build create table sql'";
        Assert.assertEquals(expected, stmt);
    }

    @Test
    public void testBuildCreateDatabaseStatement() {
        String database = "test_database";
        String stmt = dialect.buildCreateDatabaseStatement(database);
        String expected = "CREATE DATABASE IF NOT EXISTS `test_database`";
        Assert.assertEquals(expected, stmt);
    }

    @Test
    public void testBuildCreateSchemaStatement() {
        String database = "test_schema";
        String stmt = dialect.buildCreateSchemaStatement(database);
        String expected = "CREATE SCHEMA IF NOT EXISTS `test_schema`";
        Assert.assertEquals(expected, stmt);
    }

    @Test
    public void testBuildAddColumnStatement() {
        String tableIdentifier = "test_database.test_schema.test_table";
        List<RowField> fields = Arrays.asList(
                new RowField("class", new VarCharType(10), "class"),
                new RowField("name", new VarCharType(11), "name"),
                new RowField("age", new IntType(), "age"),
                new RowField("money", new DecimalType(11, 3), "money"),
                new RowField("height", new DoubleType(), "height"),
                new RowField("weight", new FloatType(), "weight"));
        String stmt = dialect.buildAlterTableStatement(tableIdentifier) + " "
                + dialect.buildAddColumnStatement(tableIdentifier, fields);
        String expected = "ALTER TABLE `test_database`.`test_schema`.`test_table` "
                + "ADD COLUMN IF NOT EXISTS `class` VARCHAR(10) COMMENT 'class',"
                + "ADD COLUMN IF NOT EXISTS `name` VARCHAR(11) COMMENT 'name',"
                + "ADD COLUMN IF NOT EXISTS `age` INTEGER COMMENT 'age',"
                + "ADD COLUMN IF NOT EXISTS `money` DECIMAL(11, 3) COMMENT 'money',"
                + "ADD COLUMN IF NOT EXISTS `height` DOUBLE PRECISION COMMENT 'height',"
                + "ADD COLUMN IF NOT EXISTS `weight` REAL COMMENT 'weight'";
        Assert.assertEquals(expected, stmt);
    }

    @Test
    public void testBuildAddColumnStatementWithPosition() {
        String tableIdentifier = "test_database.test_schema.test_table";
        List<RowField> fields = Arrays.asList(
                new RowField("class", new VarCharType(10), "class"),
                new RowField("name", new VarCharType(11), "name"),
                new RowField("age", new IntType(), "age"),
                new RowField("money", new DecimalType(11, 3), "money"),
                new RowField("height", new DoubleType(), "height"),
                new RowField("weight", new FloatType(), "weight"));
        Map<String, String> positionMap = new HashMap<>();
        positionMap.put("class", null);
        positionMap.put("name", "class");
        positionMap.put("age", "name");
        positionMap.put("money", "name");
        positionMap.put("weight", "money");
        positionMap.put("height", "weight");
        String stmt = dialect.buildAlterTableStatement(tableIdentifier) + " "
                + dialect.buildAddColumnStatement(tableIdentifier, fields, positionMap);
        String expected = "ALTER TABLE `test_database`.`test_schema`.`test_table` "
                + "ADD COLUMN IF NOT EXISTS `class` VARCHAR(10) COMMENT 'class' FIRST,"
                + "ADD COLUMN IF NOT EXISTS `name` VARCHAR(11) COMMENT 'name' AFTER `class`,"
                + "ADD COLUMN IF NOT EXISTS `age` INTEGER COMMENT 'age' AFTER `name`,"
                + "ADD COLUMN IF NOT EXISTS `money` DECIMAL(11, 3) COMMENT 'money' AFTER `name`,"
                + "ADD COLUMN IF NOT EXISTS `height` DOUBLE PRECISION COMMENT 'height' AFTER `weight`,"
                + "ADD COLUMN IF NOT EXISTS `weight` REAL COMMENT 'weight' AFTER `money`";
        Assert.assertEquals(expected, stmt);
    }
}
