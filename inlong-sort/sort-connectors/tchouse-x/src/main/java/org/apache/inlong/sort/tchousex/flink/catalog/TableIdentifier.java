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

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

public class TableIdentifier {

    private String database;
    private String table;
    private static final Splitter DOT = Splitter.on('.');

    public TableIdentifier(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public static TableIdentifier of(String... names) {
        Preconditions.checkArgument(names != null, "Cannot create table identifier from null array");
        Preconditions.checkArgument(names.length > 0, "Cannot create table identifier without a table name");
        return new TableIdentifier(names[0], names[1]);
    }

    public static TableIdentifier of(String database, String name) {
        return new TableIdentifier(database, name);
    }

    public static TableIdentifier parse(String identifier) {
        Preconditions.checkArgument(identifier != null, "Cannot parse table identifier: null");
        Iterable<String> parts = DOT.split(identifier);
        return of(Iterables.toArray(parts, String.class));
    }

    public String getFullName() {
        return "`" + database + "`.`" + table + "`";
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) o;
        return StringUtils.equalsIgnoreCase(database, that.database) && StringUtils.equalsIgnoreCase(table,
                that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(database, table);
    }

    @Override
    public String toString() {
        return "TableIdentifier{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
