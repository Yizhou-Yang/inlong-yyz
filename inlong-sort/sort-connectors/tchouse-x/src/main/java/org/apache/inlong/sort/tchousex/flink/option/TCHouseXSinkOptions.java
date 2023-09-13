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
import java.time.Duration;

public class TCHouseXSinkOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer sinkBatchSize;
    private Integer sinkMaxRetries;
    private Long sinkBatchIntervalMs;
    private Long sinkBatchBytes;

    public TCHouseXSinkOptions() {
    }

    public TCHouseXSinkOptions(Integer sinkBatchSize, Integer sinkMaxRetries, Long sinkBatchIntervalMs,
            Long sinkBatchBytes) {
        this.sinkBatchSize = sinkBatchSize;
        this.sinkMaxRetries = sinkMaxRetries;
        this.sinkBatchIntervalMs = sinkBatchIntervalMs;
        this.sinkBatchBytes = sinkBatchBytes;
    }

    public Integer getSinkBatchSize() {
        return sinkBatchSize;
    }

    public void setSinkBatchSize(Integer sinkBatchSize) {
        this.sinkBatchSize = sinkBatchSize;
    }

    public Integer getSinkMaxRetries() {
        return sinkMaxRetries;
    }

    public void setSinkMaxRetries(Integer sinkMaxRetries) {
        this.sinkMaxRetries = sinkMaxRetries;
    }

    public Long getSinkBatchIntervalMs() {
        return sinkBatchIntervalMs;
    }

    public void setSinkBatchIntervalMs(Long sinkBatchIntervalMs) {
        this.sinkBatchIntervalMs = sinkBatchIntervalMs;
    }

    public Long getSinkBatchBytes() {
        return sinkBatchBytes;
    }

    public void setSinkBatchBytes(Long sinkBatchBytes) {
        this.sinkBatchBytes = sinkBatchBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer sinkBatchSize;
        private Integer sinkMaxRetries;
        private Long sinkBatchIntervalMs;
        private Long sinkBatchBytes;

        public Builder setSinkBatchSize(Integer sinkBatchSize) {
            this.sinkBatchSize = sinkBatchSize;
            return this;
        }

        public Builder setSinkMaxRetries(Integer sinkMaxRetries) {
            this.sinkMaxRetries = sinkMaxRetries;
            return this;
        }

        public Builder setSinkBatchIntervalMs(Long sinkBatchIntervalMs) {
            this.sinkBatchIntervalMs = sinkBatchIntervalMs;
            return this;
        }

        public Builder setSinkBatchBytes(Long sinkBatchBytes) {
            this.sinkBatchBytes = sinkBatchBytes;
            return this;
        }

        public TCHouseXSinkOptions build() {
            return new TCHouseXSinkOptions(sinkBatchSize, sinkMaxRetries, sinkBatchIntervalMs, sinkBatchBytes);
        }
    }

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.batch.size")
            .intType()
            .defaultValue(1024)
            .withDescription("the flush max size, over this number"
                    + " of records, will flush data. The default value is 1024.");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.batch.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The "
                    + "default value is 1s.");
    public static final ConfigOption<Long> SINK_BUFFER_FLUSH_MAX_BYTES = ConfigOptions
            .key("sink.batch.bytes")
            .longType()
            .defaultValue(10485760L)
            .withDescription("the flush max bytes, over this number"
                    + " in batch, will flush data. The default value is 10MB.");

}
