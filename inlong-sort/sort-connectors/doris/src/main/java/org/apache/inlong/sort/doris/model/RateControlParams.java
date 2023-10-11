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

import java.io.Serializable;

public class RateControlParams implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer trendCycle;
    private Integer trendNumber;
    private Integer recoverCycle;
    private Boolean rateLimitEnable;

    public RateControlParams() {
    }

    public RateControlParams(Integer trendCycle, Integer trendNumber, Integer recoverCycle,
            Boolean rateLimitEnable) {
        this.trendCycle = trendCycle;
        this.trendNumber = trendNumber;
        this.recoverCycle = recoverCycle;
        this.rateLimitEnable = rateLimitEnable;
    }

    public Integer getTrendCycle() {
        return trendCycle;
    }

    public void setTrendCycle(Integer trendCycle) {
        this.trendCycle = trendCycle;
    }

    public Integer getTrendNumber() {
        return trendNumber;
    }

    public void setTrendNumber(Integer trendNumber) {
        this.trendNumber = trendNumber;
    }

    public Integer getRecoverCycle() {
        return recoverCycle;
    }

    public void setRecoverCycle(Integer recoverCycle) {
        this.recoverCycle = recoverCycle;
    }

    public Boolean getRateLimitEnable() {
        return rateLimitEnable;
    }

    public void setRateLimitEnable(Boolean rateLimitEnable) {
        this.rateLimitEnable = rateLimitEnable;
    }
}
