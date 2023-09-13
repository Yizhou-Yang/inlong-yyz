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

package org.apache.inlong.sort.jdbc.options;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.jdbc.dialect.PostgresDialect;

import java.io.Serializable;
import java.util.Objects;

/**
 * JDBC sink batch options.
 */
@PublicEvolving
public class JdbcExecutionOptions implements Serializable {

    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    public static final int DEFAULT_SIZE = 5000;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;
    // 50mb
    private static final long DEFAULT_SINK_BATCH_BYTES = 52428800;
    private final long batchIntervalMs;
    private final int batchSize;
    private final int maxRetries;
    private final long sinkBatchBytes;
    private final String writeMode; // insert or copy
    private final String copyDelimiter; // copy delimiter

    private JdbcExecutionOptions(long batchIntervalMs, int batchSize, int maxRetries, String writeMode,
            String copyDelimiter, long sinkBatchBytes) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.batchIntervalMs = batchIntervalMs;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.writeMode = writeMode;
        this.copyDelimiter = copyDelimiter;
        this.sinkBatchBytes = sinkBatchBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JdbcExecutionOptions defaults() {
        return builder().build();
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public String getCopyDelimiter() {
        return copyDelimiter;
    }

    public long getSinkBatchBytes() {
        return sinkBatchBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcExecutionOptions that = (JdbcExecutionOptions) o;
        return batchIntervalMs == that.batchIntervalMs
                && batchSize == that.batchSize
                && maxRetries == that.maxRetries
                && writeMode.equals(that.writeMode)
                && copyDelimiter.equals(that.copyDelimiter)
                && sinkBatchBytes == that.sinkBatchBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchIntervalMs, batchSize, maxRetries, writeMode, copyDelimiter, sinkBatchBytes);
    }

    /**
     * Builder for {@link JdbcExecutionOptions}.
     */
    public static final class Builder {

        private long intervalMs = DEFAULT_INTERVAL_MILLIS;
        private int size = DEFAULT_SIZE;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private String writeMode = PostgresDialect.WRITE_MODE_COPY;
        private String copyDelimiter = PostgresDialect.DEFAULT_COPY_DELIMITER;
        private long sinkBatchBytes = DEFAULT_SINK_BATCH_BYTES;

        public Builder withBatchSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withBatchIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder withWriteMode(String writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder withCopyDelimiter(String copyDelimiter) {
            this.copyDelimiter = copyDelimiter;
            return this;
        }

        public Builder withSinkBatchBytes(long sinkBatchBytes) {
            this.sinkBatchBytes = sinkBatchBytes;
            return this;
        }

        public JdbcExecutionOptions build() {
            return new JdbcExecutionOptions(intervalMs, size, maxRetries, writeMode, copyDelimiter, sinkBatchBytes);
        }
    }
}
