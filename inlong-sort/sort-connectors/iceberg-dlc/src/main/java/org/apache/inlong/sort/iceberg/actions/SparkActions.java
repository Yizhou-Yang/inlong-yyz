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

package org.apache.inlong.sort.iceberg.actions;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.inlong.sort.iceberg.FlinkActions;

import java.util.Map;

@Slf4j
public class SparkActions implements FlinkActions {
    private static final long serialVersionUID = 1L;

    private Map<String, String> actionProperties;

    @Override
    public void init(Map<String, String> actionProperties) {
        this.actionProperties = actionProperties;
    }

    @Override
    public RewriteDataFiles rewriteDataFiles(Table table) {
        return new SyncRewriteDataFilesAction(
                new SyncRewriteDataFilesActionOption(actionProperties),
                table);
    }

    public Map<String, String> getActionProperties() {
        return actionProperties;
    }
}
