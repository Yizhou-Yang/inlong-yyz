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

package org.apache.inlong.sort.kafka.partitioner;

import lombok.Setter;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.inlong.sort.base.format.AbstractDynamicSchemaFormat;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class InLongFixedPartitionPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InLongFixedPartitionPartitioner.class);

    private final Map<String, String> patternPartitionMap;
    private final Map<String, Pattern> regexPatternMap;
    private AbstractDynamicSchemaFormat dynamicSchemaFormat;

    /**
     * The format used to deserialization the raw data(bytes array)
     */
    @Setter
    private String sinkMultipleFormat;

    private final String DEFAULT_PARTITION = "DEFAULT_PARTITION";

    private String databasePattern;
    private String tablePattern;

    private final static String DELIMITER1 = "&";
    private final static String DELIMITER2 = "_";

    public InLongFixedPartitionPartitioner(Map<String, String> patternPartitionMap, String partitionPattern) {
        this.patternPartitionMap = patternPartitionMap;
        this.regexPatternMap = new HashMap<>();
        this.databasePattern = partitionPattern.split(DELIMITER2)[0];
        this.tablePattern = partitionPattern.split(DELIMITER2)[1];
    }

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        super.open(parallelInstanceId, parallelInstances);
        dynamicSchemaFormat = DynamicSchemaFormatFactory.getFormat(sinkMultipleFormat);
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        int partition = 0;
        try {
            partition = Integer.parseInt(patternPartitionMap.getOrDefault(DEFAULT_PARTITION, "0"));
        } catch (Exception e) {
            LOG.error("ParseInt for DEFAULT_PARTITION  error， default partition use 0", e);
        }

        try {
            for (Map.Entry<String, String> entry : patternPartitionMap.entrySet()) {
                if (DEFAULT_PARTITION.equals(entry.getKey())) {
                    continue;
                }
                String databaseName = dynamicSchemaFormat.parse(value, databasePattern);
                String tableName = dynamicSchemaFormat.parse(value, tablePattern);
                List<String> regexList = Arrays.asList(entry.getKey().split(DELIMITER1));
                String databaseNameRegex = regexList.get(0);
                String tableNameRegex = regexList.get(1);

                if (match(databaseName, databaseNameRegex) && match(tableName, tableNameRegex)) {
                    return Integer.parseInt(entry.getValue());
                }
            }
        } catch (Exception e) {
            LOG.warn("Extract partition failed", e);
        }
        return partition;
    }

    private boolean match(String name, String nameRegex) {
        return regexPatternMap.computeIfAbsent(nameRegex, regex -> Pattern.compile(regex))
                .matcher(name)
                .matches();
    }
}
