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

package org.apache.inlong.sort.doris.schema.util;

import org.apache.inlong.sort.doris.model.RateControlParams;
import org.apache.inlong.sort.doris.util.RateLimitUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RateLimitUtilsTest {

    @Test
    public void testUpdateTableMapRateLimitFactor() {
        String tableName = "table1";
        Integer confMaxTabletVersionNum = 100;
        Map<String, List<Integer>> tableMapTrendCycleVersion = new HashMap<>();
        Map<String, List<Integer>> tableMapVersionAverageValue = new HashMap<>();
        Map<String, Integer> tableMapKeepLowRateTimes = new HashMap<>();
        Map<String, Integer> tableMapRateLimitFactor = new HashMap<>();
        RateControlParams rateControlParams = new RateControlParams(10, 3, 5, true);
        int[] version = {10, 11, 12, 13, 20, 21, 22, 23, 23, 24,
                25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
                35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
                38, 37, 36, 35, 34, 33, 32, 31, 30, 29,
                29, 28, 27, 26, 25, 24, 11, 10, 9, 8,
                10, 10, 10, 11, 12, 13, 14, 15, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
                37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
                47, 47, 48, 45, 44, 41, 45, 46, 47, 48,
                49, 50, 51, 52, 54, 55, 56, 61, 62, 63,
                55, 54, 53, 52, 51, 51, 50, 49, 49, 48,
                40, 39, 34, 32, 21, 15, 10, 11, 10, 11,
                10, 9, 9, 9, 9, 50, 50, 9, 8, 8};
        for (int i = 0; i < 140; i++) {
            RateLimitUtils.updateTableMapRateLimitFactor(tableName, tableMapTrendCycleVersion,
                    tableMapVersionAverageValue, tableMapKeepLowRateTimes, tableMapRateLimitFactor,
                    rateControlParams, version[i], confMaxTabletVersionNum);
            System.out.println("tableMapRateLimitFactor:" + tableMapRateLimitFactor);
            System.out.println("tableMapTrendCycleVersion:" + tableMapTrendCycleVersion);
            System.out.println("tableMapVersionAverageValue:" + tableMapVersionAverageValue);
            System.out.println("tableMapKeepLowRateTimes:" + tableMapKeepLowRateTimes);
            System.out.println("\n");
        }
    }

}
