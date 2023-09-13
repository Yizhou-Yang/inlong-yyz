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

package org.apache.inlong.sort.tchousex.flink.option;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

public class TCHouseXOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private String hostName;
    private Integer port;
    private String username;
    private String password;
    private String tableIdentifier;

    public TCHouseXOptions() {
    }

    public TCHouseXOptions(String hostName, Integer port, String username, String password,
            String tableIdentifier) {
        this.hostName = hostName;
        this.port = port;
        this.username = username;
        this.password = password;
        this.tableIdentifier = tableIdentifier;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String hostName;
        private Integer port;
        private String username;
        private String password;
        private String tableIdentifier;

        public Builder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public TCHouseXOptions build() {
            return new TCHouseXOptions(hostName, port, username, password, tableIdentifier);
        }

    }

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the TCHouse-X database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(33060)
                    .withDescription("Integer port number of the TCHouse-X database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the TCHouse-X database to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the TCHouse-X database server.");

    public static final ConfigOption<String> TABLE_IDENTIFIER =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue().withDescription("the jdbc table name.");

}
