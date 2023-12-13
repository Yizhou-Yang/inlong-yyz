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

package org.apache.inlong.sort.iceberg.sink.multiple;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IcebergMultipleFilesCommiter extends IcebergProcessFunction<MultipleWriteResult, Void>
        implements
            CheckpointedFunction,
            CheckpointListener,
            BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMultipleFilesCommiter.class);

    private ConcurrentHashMap<TableIdentifier, IcebergSingleFileCommiter> multipleCommiters;
    private final CatalogLoader catalogLoader;
    private final boolean overwrite;
    private final ActionsProvider actionsProvider;

    private final int commitConcurrency;

    private final Duration commitTimeout;

    private final ReadableConfig tableOptions;

    private transient FunctionInitializationContext functionInitializationContext;

    private transient ExecutorService commitExecutors;

    public IcebergMultipleFilesCommiter(
            CatalogLoader catalogLoader,
            boolean overwrite,
            ActionsProvider actionProvider,
            int commitConcurrency,
            Duration commitTimeout,
            ReadableConfig tableOptions) {
        this.catalogLoader = catalogLoader;
        this.overwrite = overwrite;
        this.actionsProvider = actionProvider;
        this.commitConcurrency = commitConcurrency;
        this.commitTimeout = commitTimeout;
        this.tableOptions = tableOptions;
    }

    @Override
    public void processElement(MultipleWriteResult value) throws Exception {
        TableIdentifier tableId = value.getTableId();
        if (multipleCommiters.get(tableId) == null) {
            IcebergSingleFileCommiter commiter =
                    new IcebergSingleFileCommiter(
                            tableId,
                            TableLoader.fromCatalog(catalogLoader, value.getTableId()),
                            overwrite,
                            actionsProvider,
                            tableOptions);
            commiter.setup(getRuntimeContext(), collector, context);
            commiter.initializeState(functionInitializationContext);
            commiter.open(new Configuration());
            multipleCommiters.put(tableId, commiter);
        }

        multipleCommiters.get(tableId).processElement(value.getWriteResult());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (Entry<TableIdentifier, IcebergSingleFileCommiter> entry : multipleCommiters.entrySet()) {
            entry.getValue().snapshotState(context);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.functionInitializationContext = context;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        List<Tuple2<TableIdentifier, Future<Exception>>> tables = new ArrayList<>();

        for (Entry<TableIdentifier, IcebergSingleFileCommiter> entry : multipleCommiters.entrySet()) {
            Future<Exception> future =
                    commitExecutors.submit(
                            () -> {
                                try {
                                    LOG.info(
                                            "Start committing {} for checkpoint {}",
                                            entry.getKey(),
                                            checkpointId);
                                    entry.getValue().notifyCheckpointComplete(checkpointId);
                                    LOG.info(
                                            "Finish committing {} for checkpoint {}",
                                            entry.getKey(),
                                            checkpointId);
                                    return null;
                                } catch (Exception e) {
                                    LOG.warn(
                                            "committing {} for checkpoint {} failed",
                                            entry.getKey(),
                                            checkpointId,
                                            e);
                                    return e;
                                }
                            });
            tables.add(new Tuple2<>(entry.getKey(), future));
        }

        for (Tuple2<TableIdentifier, Future<Exception>> t : tables) {
            Exception e = t.f1.get(commitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (e != null) {
                throw e;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        multipleCommiters = new ConcurrentHashMap<>();
        commitExecutors = Executors.newFixedThreadPool(commitConcurrency);
    }

    @Override
    public void close() throws Exception {
        if (commitExecutors != null) {
            commitExecutors.shutdownNow();
        }

        if (multipleCommiters == null) {
            return;
        }

        for (Entry<TableIdentifier, IcebergSingleFileCommiter> entry : multipleCommiters.entrySet()) {
            entry.getValue().close();
        }
    }

    @Override
    public void endInput() throws Exception {
        for (Entry<TableIdentifier, IcebergSingleFileCommiter> entry : multipleCommiters.entrySet()) {
            entry.getValue().endInput();
        }
    }
}
