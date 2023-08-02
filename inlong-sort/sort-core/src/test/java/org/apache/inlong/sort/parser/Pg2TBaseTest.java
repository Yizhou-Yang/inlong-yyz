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

package org.apache.inlong.sort.parser;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.VarBinaryFormatInfo;
import org.apache.inlong.sort.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.parser.result.ParseResult;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;
import org.apache.inlong.sort.protocol.node.load.PostgresLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Pg2TBaseTest {

    private PostgresExtractNode buildInputNode() {

        Map<String, String> option = new HashMap<>();
        option.put("source.multiple.enable", "true");
        // option.put("scan.incremental.snapshot.enabled", "true");
        option.put("debezium.binary.handling.mode", "base64");
        option.put("debezium.snapshot.locking.mode", "none");
        option.put("debezium.snapshot.mode", "initial");
        List<String> tables = new ArrayList(10);
        tables.add("public.test_name");
        List<FieldInfo> fields = Collections.singletonList(
                new MetaFieldInfo("raw", MetaField.DATA));
        return new PostgresExtractNode("1", "mysql_input", fields,
                null, option, null,
                tables, "101.42.138.203", "test", "Wedata@2022",
                "wedata_dev", "public", 5432, null,
                null, null);
    }

    private PostgresLoadNode buildOutputNode() {
        List<FieldInfo> fields = Collections.singletonList(new FieldInfo("raw", new VarBinaryFormatInfo()));
        List<FieldRelation> relations = Collections
                .singletonList(new FieldRelation(new FieldInfo("raw", new VarBinaryFormatInfo()),
                        new FieldInfo("raw", new StringFormatInfo())));
        Map<String, String> option = new HashMap<>();
        option.put("sink.multiple.schema-update.policy", "THROW_WITH_STOP");
        option.put("sink.multiple.enable", "true");
        option.put("sink.multiple.format", "canal-json");
        option.put("sink.multiple.database-pattern", "at_tbase_bj");
        option.put("sink.multiple.table-pattern", "${table}");
        option.put("sink.multiple.schema-pattern", "public");
        return new PostgresLoadNode("2", "mysql_input", fields, relations, null,
                null, null, option, "jdbc:postgresql://106.52.220.38:25432",
                "wedata", "Wedata@2022", "notuse", null);
    }

    private NodeRelation buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelation(inputIds, outputIds);
    }

    /**
     * Test all migrate, the full database data is represented as a canal string
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testAllMigrate() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Node inputNode = buildInputNode();
        Node outputNode = buildOutputNode();
        StreamInfo streamInfo = new StreamInfo("1", Arrays.asList(inputNode, outputNode),
                Collections.singletonList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();
        result.waitExecute();
        Assert.assertTrue(result.tryExecute());
    }
}
