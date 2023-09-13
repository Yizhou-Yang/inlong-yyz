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

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.inlong.sort.tchousex.flink.config.TCHouseXConfig;
import org.apache.inlong.sort.tchousex.flink.connection.JdbcClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TCHouseXCatalogTest {

    TCHouseXCatalog tcHouseXCatalog;

    @Ignore
    @Before
    public void init() {
        TCHouseXConfig tcHouseXConfig = new TCHouseXConfig("", 33060, "", "");
        JdbcClient jdbcClient = new JdbcClient(tcHouseXConfig);
        tcHouseXCatalog = new TCHouseXCatalog(jdbcClient);
    }

    @Ignore
    @Test
    public void testListDatabases() {
        List<String> databaseList = tcHouseXCatalog.listDatabases();
        System.out.println(databaseList);
    }

    @Ignore
    @Test
    public void testDatabaseExists() {
        boolean flag = tcHouseXCatalog.databaseExists("menghuiyu1");
        System.out.println(flag);
    }

    @Ignore
    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> all = tcHouseXCatalog.listTables("menghuiyu1");
        System.out.println(all);
    }

    @Ignore
    @Test
    public void testGetTable() {
        Table table3 = tcHouseXCatalog.getTable(new TableIdentifier("pacino_test", "di291_user_partiton"));
        System.out.println(table3.getNormalFields());
        System.out.println(table3.getPartitionFields());
    }

    @Ignore
    @Test
    public void testTableExists() {
        System.out.println(tcHouseXCatalog.tableExists(new TableIdentifier("menghuiyu1", "tb1")));
    }

    @Ignore
    @Test
    public void testDropTable() {
        tcHouseXCatalog.dropTable(new TableIdentifier("menghuiyu1", "dwd_tm_user_profile_df"), true);
    }

    @Ignore
    @Test
    public void testCreateTable() {
        TableIdentifier tableIdentifier = new TableIdentifier("pacino_test4", "dwd_tm_user_profile_df");
        Table table = new Table();
        table.setTableIdentifier(tableIdentifier);
        List<Field> normalFieldList = new ArrayList<>();
        normalFieldList.add(new Field("uid", "STRING"));
        normalFieldList.add(new Field("ip_province", "STRING"));
        normalFieldList.add(new Field("ip_city", "STRING"));
        normalFieldList.add(new Field("test_1", "decimal(10,2)"));
        table.setNormalFields(normalFieldList);
        tcHouseXCatalog.createTable(table, true);
    }
}
