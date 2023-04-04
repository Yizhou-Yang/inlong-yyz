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

import java.util.Map;

public class InLongFixedPartitionPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InLongFixedPartitionPartitioner.class);

    private final Map<String, String> patternPartitionMap;
    private AbstractDynamicSchemaFormat dynamicSchemaFormat;

    /**
     * The format used to deserialization the raw data(bytes array)
     */
    @Setter
    private String sinkMultipleFormat;

    private final String DEFAULT_PARTITION = "DEFAULT_PARTITION";

    public InLongFixedPartitionPartitioner(Map<String, String> patternPartitionMap) {
        this.patternPartitionMap = patternPartitionMap;
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
            for (Map.Entry<String, String> entry : patternPartitionMap.entrySet()) {
                if (DEFAULT_PARTITION.equals(entry.getKey())) {
                    continue;
                }
                String partitionKey = dynamicSchemaFormat.parse(value, entry.getKey());
                if (partitionKey != null && !partitionKey.isEmpty()) {
                    return Integer.parseInt(entry.getValue());
                }
            }
            return Integer.parseInt(patternPartitionMap.getOrDefault(DEFAULT_PARTITION, "0"));
        } catch (Exception e) {
            LOG.warn("Extract partition failed", e);
        }
        return partition;
    }
}
