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

package org.apache.inlong.sort.doris.model;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * The class describes the version info of Doris
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DorisVersion {

    private FeVersionInfo feVersionInfo;

    /**
     * Check if supports DecimalV3. The DecimalV3 is supported by Doris after 1.2.1
     * @return true if support DecimalV3 else false
     */
    public boolean supportDecimalV3() {
        if (feVersionInfo == null || feVersionInfo.getDorisBuildVersionMajor() < 1) {
            return false;
        }
        if (feVersionInfo.getDorisBuildVersionMajor() > 1) {
            return true;
        }
        if (feVersionInfo.getDorisBuildVersionMinor() > 2) {
            return true;
        }
        if (feVersionInfo.getDorisBuildVersionMinor() < 2) {
            return false;
        }
        return feVersionInfo.getDorisBuildVersionPatch() >= 1;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FeVersionInfo {

        private String dorisBuildTime;
        private String dorisBuildShortHash;
        private String dorisJavaCompileVersion;
        private Integer dorisBuildVersionMajor = 0;
        private Integer dorisBuildVersionMinor = 0;
        private String dorisBuildVersionPrefix;
        private String dorisBuildVersionRcVersion;
        private String dorisBuildVersion;
        private String dorisBuildInfo;
        private Integer dorisBuildVersionPatch = 0;
        private String dorisBuildHash;
    }
}
