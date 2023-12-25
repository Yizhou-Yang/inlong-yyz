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

package org.apache.inlong.sort.protocol.constant;

import lombok.Getter;

public class OceanBaseConstant {

    /**
     * The key of flink connector defined in flink table
     */
    public static final String CONNECTOR = "connector";

    /**
     * Specify what flink connector to use for extract data from oceanBase database, here should be 'oceanbase-cdc'
     */
    public static final String OCEANBASE_CDC = "oceanbase-cdc-inlong";

    public static final String HOSTNAME = "hostname";

    public static final String PORT = "port";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String TENANT_NAME = "tenant-name";

    public static final String DATABASE_NAME = "database-name";

    public static final String TABLE_NAME = "table-name";

    public static final String TABLE_LIST = "table-list";

    /**
     * The key of ${@link ScanStartUpMode}
     */
    public static final String SCAN_STARTUP_MODE = "scan.startup.mode";

    /**
     * Optional startup mode for Oracle CDC consumer,
     * valid enumerations are "initial" and "latest-offset".
     * Please see Startup Reading Positionsection for more detailed information.
     */
    @Getter
    public enum ScanStartUpMode {

        /**
         * Performs an initial snapshot on the monitored database tables upon first startup,
         * and continue to read the latest binlog.
         */
        INITIAL("initial"),
        /**
         * Never to perform a snapshot on the monitored database tables upon first startup,
         * just read from the change since the connector was started.
         */
        LATEST_OFFSET("latest-offset"),

        TIMESTAMP("timestamp");

        final String value;

        ScanStartUpMode(String value) {
            this.value = value;
        }

        public static ScanStartUpMode forName(String name) {
            for (ScanStartUpMode dataType : ScanStartUpMode.values()) {
                if (dataType.getValue().equals(name)) {
                    return dataType;
                }
            }
            throw new IllegalArgumentException(String.format("Unsupport ScanStartUpMode for oceanBase source:%s",
                    name));
        }
    }

}
