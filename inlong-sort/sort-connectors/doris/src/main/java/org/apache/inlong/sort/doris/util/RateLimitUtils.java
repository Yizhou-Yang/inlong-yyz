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

package org.apache.inlong.sort.doris.util;

import org.apache.inlong.sort.doris.model.RateControlParams;
import org.apache.inlong.sort.doris.model.TrendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * limit write rate for doris
 * refer to <a href="https://iwiki.woa.com/p/4008576712">Inlong导入Doris状态反馈机制</a>
 */
public class RateLimitUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitUtils.class);
    private static final double baseRateFactor = 0.1d;

    public static void waite(Integer factor, Long intervalMs) {
        if (factor == null || factor == 0 || intervalMs == 0L) {
            return;
        }
        try {
            long waiteTime = factor * intervalMs;
            LOG.warn("rate limit waite:{}ms", waiteTime);
            Thread.sleep(waiteTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void updateTableMapTrendCycleVersion(String tableName,
            Map<String, List<Integer>> tableMapTrendCycleVersion, int tableMaxTabletVersionCount,
            int cycleNum) {
        List<Integer> cycleVersionList = tableMapTrendCycleVersion.get(tableName);
        if (cycleVersionList == null) {
            cycleVersionList = new ArrayList<>();
            cycleVersionList.add(tableMaxTabletVersionCount);
            tableMapTrendCycleVersion.put(tableName, cycleVersionList);
        } else {
            if (cycleVersionList.size() >= cycleNum) {
                cycleVersionList.clear();
            }
            cycleVersionList.add(tableMaxTabletVersionCount);
        }
    }

    private static void updateTableMapVersionAverageValue(String tableName,
            Map<String, List<Integer>> tableMapTrendCycleVersion,
            Map<String, List<Integer>> tableMapVersionAverageValue,
            int cycleNum, int trendNumber) {
        List<Integer> cycleVersionList = tableMapTrendCycleVersion.get(tableName);
        if (cycleVersionList.size() == cycleNum) {
            // compute average value
            Integer sum = 0;
            for (Integer version : cycleVersionList) {
                sum += version;
            }
            Integer averageValue = sum / cycleNum;
            List<Integer> versionAverageValueList = tableMapVersionAverageValue.get(tableName);
            if (versionAverageValueList == null) {
                versionAverageValueList = new ArrayList<>();
                tableMapVersionAverageValue.put(tableName, versionAverageValueList);
            } else {
                if (versionAverageValueList.size() == trendNumber) {
                    versionAverageValueList.remove(0);
                }
            }
            versionAverageValueList.add(averageValue);
        }
    }

    private static TrendStatus getTrendStatus(String tableName,
            Map<String, List<Integer>> tableMapVersionAverageValue, int trendNumber) {
        List<Integer> versionAverageValueList = tableMapVersionAverageValue.get(tableName);
        if (versionAverageValueList == null || versionAverageValueList.isEmpty()
                || versionAverageValueList.size() < trendNumber) {
            return TrendStatus.STABLE;
        }
        int firstDiffValue = versionAverageValueList.get(1) - versionAverageValueList.get(0);
        for (int i = 2; i < versionAverageValueList.size(); i++) {
            int diff = versionAverageValueList.get(i) - versionAverageValueList.get(i - 1);
            if (firstDiffValue >= 0 && diff <= 0) {
                return TrendStatus.STABLE;
            }
            if (firstDiffValue <= 0 && diff >= 0) {
                return TrendStatus.STABLE;
            }
        }
        if (firstDiffValue > 0) {
            return TrendStatus.RISE;
        } else {
            return TrendStatus.DECLINE;
        }
    }

    private static int updateTableMapKeepLowRateTimes(String tableName, Map<String, Integer> tableMapKeepLowRateTimes,
            int recoverTimes) {
        Integer rateTimes = tableMapKeepLowRateTimes.get(tableName);
        if (rateTimes == null) {
            tableMapKeepLowRateTimes.put(tableName, recoverTimes);
        } else {
            if (rateTimes < recoverTimes) {
                tableMapKeepLowRateTimes.put(tableName, rateTimes + 1);
            }
        }
        return tableMapKeepLowRateTimes.get(tableName);
    }

    public static void updateTableMapRateLimitFactor(String tableName,
            Map<String, List<Integer>> tableMapTrendCycleVersion,
            Map<String, List<Integer>> tableMapVersionAverageValue,
            Map<String, Integer> tableMapKeepLowRateTimes,
            Map<String, Integer> tableMapRateLimitFactor,
            RateControlParams rateControlParams, Integer tableMaxTabletVersionCount,
            Integer confMaxTabletVersionNum) {
        updateTableMapTrendCycleVersion(tableName, tableMapTrendCycleVersion, tableMaxTabletVersionCount,
                rateControlParams.getTrendCycle());
        updateTableMapVersionAverageValue(tableName, tableMapTrendCycleVersion, tableMapVersionAverageValue,
                rateControlParams.getTrendCycle(), rateControlParams.getTrendNumber());
        double currentRate = 0;
        if (confMaxTabletVersionNum > 0 && tableMaxTabletVersionCount > 0) {
            currentRate = (double) tableMaxTabletVersionCount / confMaxTabletVersionNum;
        }
        TrendStatus status = getTrendStatus(tableName, tableMapVersionAverageValue,
                rateControlParams.getTrendNumber());
        int tableRateFactor = (int) (currentRate * 10);
        if (currentRate <= baseRateFactor) {
            int lowRateTimes = updateTableMapKeepLowRateTimes(tableName, tableMapKeepLowRateTimes,
                    rateControlParams.getRecoverCycle());
            if ((TrendStatus.STABLE == status || TrendStatus.DECLINE == status)
                    && lowRateTimes >= rateControlParams.getRecoverCycle()) {
                tableMapRateLimitFactor.put(tableName, 0);
            }
        } else {
            tableMapKeepLowRateTimes.put(tableName, 0);
            if (TrendStatus.RISE == status || TrendStatus.DECLINE == status) {
                tableMapRateLimitFactor.put(tableName, tableRateFactor);
            }
        }
    }

}
