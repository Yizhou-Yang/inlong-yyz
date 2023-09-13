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

package org.apache.inlong.sort.tchousex.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TaskRunUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRunUtil.class);

    public static void run(Task task, Integer maxAttempts) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                task.run();
                return;
            } catch (Throwable e) {
                LOG.warn("Retrying task after failure: {}", e.getMessage(), e);
                if (attempt >= maxAttempts) {
                    throw new RuntimeException(e);
                }
                try {
                    LOG.warn("wait {}s for next retry", attempt);
                    TimeUnit.SECONDS.sleep(attempt);
                } catch (InterruptedException ex) {
                    LOG.warn("", ex);
                }
            }
        }
    }

    public interface Task {

        void run() throws Throwable;
    }

}
