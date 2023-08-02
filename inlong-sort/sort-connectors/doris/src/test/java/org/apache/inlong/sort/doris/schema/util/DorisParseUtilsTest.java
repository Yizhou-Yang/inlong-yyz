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

import org.apache.inlong.sort.doris.util.DorisParseUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for {@link DorisParseUtils}
 */
public class DorisParseUtilsTest {

    @Test
    public void testParse() {
        String message = "stream load error: errCode = 7, detailMessage = unknown table, tableName=act_ge_bytearray";
        Assert.assertTrue(DorisParseUtils.parseUnkownTableError(message));
        List<String> alreadyExists = Arrays.asList(
                "errCode = 2, detailMessage = Can not add column which already exists in base table: c1",
                "errCode = 2, detailMessage = Table 'student' already exists2",
                "errCode = 2, detailMessage = Can't create database 'default_cluster:wedata_dev_2'; database exists");
        for (String s : alreadyExists) {
            Assert.assertTrue(DorisParseUtils.parseAlreadyExistsError(s));
        }
    }
}
