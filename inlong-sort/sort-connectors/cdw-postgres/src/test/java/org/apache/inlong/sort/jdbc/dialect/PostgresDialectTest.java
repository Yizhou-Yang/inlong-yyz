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

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class PostgresDialectTest {

    private PostgresDialect dialect;

    @Before
    public void init() {
        dialect = new PostgresDialect();
    }

    @Test
    public void testIsResourceNotExists() {
        SQLException e = new SQLException(
                "org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor.executeBatch(TableCopyStatementExecutor.java:202) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor.executeBatch(TableBufferedStatementExecutor.java:64) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat.flushTable(JdbcMultiBatchingOutputFormat.java:816) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat.lambda$attemptFlush$4(JdbcMultiBatchingOutputFormat.java:768) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_382]\n"
                        + "\tat java.lang.Thread.run(Thread.java:750) [?:1.8.0_382]\n"
                        + "Caused by: java.util.concurrent.ExecutionException: org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat java.util.concurrent.FutureTask.report(FutureTask.java:122) ~[?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.FutureTask.get(FutureTask.java:192) ~[?:1.8.0_382]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor.executeBatch(TableCopyStatementExecutor.java:197) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\t... 9 more\n"
                        + "Caused by: org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2676) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.processCopyResults(QueryExecutorImpl.java:1264) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.startCopy(QueryExecutorImpl.java:946) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:45) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:220) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:203) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor$1.call(TableCopyStatementExecutor.java:149) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor$1.call(TableCopyStatementExecutor.java:144) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\t... 6 more\n"
                        + "2023-11-13 20:41:09.463 [pool-10-thread-1] WARN  org.apache.inlong.sort.jdbc.schema.JdbcSchemaSchangeHelper [] - Can't handle the error\n"
                        + "java.sql.SQLException: java.util.concurrent.ExecutionException: org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor.executeBatch(TableCopyStatementExecutor.java:202) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor.executeBatch(TableBufferedStatementExecutor.java:64) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat.flushTable(JdbcMultiBatchingOutputFormat.java:816) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat.lambda$attemptFlush$4(JdbcMultiBatchingOutputFormat.java:768) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_382]\n"
                        + "\tat java.lang.Thread.run(Thread.java:750) [?:1.8.0_382]\n"
                        + "Caused by: java.util.concurrent.ExecutionException: org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat java.util.concurrent.FutureTask.report(FutureTask.java:122) ~[?:1.8.0_382]\n"
                        + "\tat java.util.concurrent.FutureTask.get(FutureTask.java:192) ~[?:1.8.0_382]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor.executeBatch(TableCopyStatementExecutor.java:197) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\t... 9 more\n"
                        + "Caused by: org.postgresql.util.PSQLException: ERROR: column \"NAME\" of relation \"TABLE_ABB1\" does not exist\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2676) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.processCopyResults(QueryExecutorImpl.java:1264) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.core.v3.QueryExecutorImpl.startCopy(QueryExecutorImpl.java:946) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:45) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:220) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.postgresql.copy.CopyManager.copyIn(CopyManager.java:203) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor$1.call(TableCopyStatementExecutor.java:149) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]\n"
                        + "\tat org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor$1.call(TableCopyStatementExecutor.java:144) ~[pool-3-thread-6-1699879048496-test-sort-connector-cdw-postgres-2.10.0-1.6.0.jar:1.6.0]");
        System.out.println(dialect.isResourceNotExists(e));
    }

}
