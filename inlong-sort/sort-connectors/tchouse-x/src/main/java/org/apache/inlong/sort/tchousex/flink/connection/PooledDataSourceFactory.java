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

package org.apache.inlong.sort.tchousex.flink.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.inlong.sort.tchousex.flink.config.TCHouseXConfig;

public class PooledDataSourceFactory {

    private static final int MINIMUM_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 10;
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final long CONNECTION_TIMEOUT = 60000;
    private static final String CONNECTION_POOL_PREFIX = "tchouse-x-jdbc-pool";
    private static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%s/?useUnicode=true&rewriteBatchedStatements=true";;

    public static HikariDataSource createPooledDataSource(TCHouseXConfig tcHouseXConfig) {
        final HikariConfig config = new HikariConfig();
        String hostName = tcHouseXConfig.getHostname();
        int port = tcHouseXConfig.getPort();
        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(String.format(JDBC_URL_PATTERN, hostName, port));
        config.setUsername(tcHouseXConfig.getUsername());
        config.setPassword(tcHouseXConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setConnectionTimeout(CONNECTION_TIMEOUT);
        config.setDriverClassName(JDBC_DRIVER);
        return new HikariDataSource(config);
    }

}
