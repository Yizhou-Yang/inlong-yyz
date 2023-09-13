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

package org.apache.inlong.sort.tchousex.flink.function;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.tchousex.flink.table.TCHouseXDynamicOutputFormat;

import javax.annotation.Nonnull;

public class GenericTCHouseXSinkFunction<T> extends RichSinkFunction<T>
        implements
            CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private final TCHouseXDynamicOutputFormat<T> outputFormat;

    public GenericTCHouseXSinkFunction(@Nonnull TCHouseXDynamicOutputFormat<T> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    /**
     * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to
     * the function to ensure that all state is exposed by means previously offered through {@link
     * FunctionInitializationContext} when the Function was initialized, or offered now by {@link
     * FunctionSnapshotContext} itself.
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception Thrown, if state could not be created ot restored.
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
        outputFormat.snapshotState(context);
    }

    /**
     * This method is called when the parallel function instance is created during distributed
     * execution. Functions typically set up their state storing data structures in this method.
     *
     * @param context the context for initializing the operator
     * @throws Exception Thrown, if state could not be created ot restored.
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        outputFormat.setRuntimeContext(getRuntimeContext());
        outputFormat.initializeState(context);
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
        super.close();
    }
}
