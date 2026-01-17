/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;

import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_TRACE_DIR;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_TRACE_ENABLED;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_TRACE_MAX_FILE_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_TRACE_SYNC_FILE;

/** Configuration holder that controls RocksDB block cache tracing. */
class RocksDBBlockCacheTraceOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean enabled;
    @Nullable private final String configuredDirectory;
    private final long maxFileSizeBytes;
    private final boolean syncOnFlush;

    private RocksDBBlockCacheTraceOptions(
            boolean enabled,
            @Nullable String configuredDirectory,
            long maxFileSizeBytes,
            boolean syncOnFlush) {
        this.enabled = enabled;
        this.configuredDirectory = configuredDirectory;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.syncOnFlush = syncOnFlush;
    }

    static RocksDBBlockCacheTraceOptions disabled() {
        return new RocksDBBlockCacheTraceOptions(false, null, 0L, false);
    }

    static RocksDBBlockCacheTraceOptions fromConfig(ReadableConfig config) {
        final boolean enabled = config.get(BLOCK_CACHE_TRACE_ENABLED);
        if (!enabled) {
            return disabled();
        }
        final MemorySize maxSize = config.get(BLOCK_CACHE_TRACE_MAX_FILE_SIZE);
        final String directory = config.get(BLOCK_CACHE_TRACE_DIR);
        final boolean syncOnFlush = config.get(BLOCK_CACHE_TRACE_SYNC_FILE);
        return new RocksDBBlockCacheTraceOptions(
                true,
                StringUtils.isNullOrWhitespaceOnly(directory) ? null : directory,
                maxSize.getBytes(),
                syncOnFlush);
    }

    boolean isEnabled() {
        return enabled;
    }

    long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    boolean isSyncOnFlush() {
        return syncOnFlush;
    }

    File resolveOutputDirectory(File instanceBasePath, @Nullable JobID jobId) {
        final File baseDirectory;
        if (configuredDirectory == null) {
            baseDirectory = new File(instanceBasePath, "block-cache-traces");
        } else {
            File configured = new File(configuredDirectory);
            baseDirectory =
                    configured.isAbsolute()
                            ? configured
                            : new File(instanceBasePath, configuredDirectory);
        }
        if (jobId == null || isWithinBackendDirectory(baseDirectory, instanceBasePath)) {
            return baseDirectory;
        }
        return new File(baseDirectory, jobId.toHexString());
    }

    private boolean isWithinBackendDirectory(File target, File backendBasePath) {
        Path targetPath = target.toPath().toAbsolutePath().normalize();
        Path backendPath = backendBasePath.toPath().toAbsolutePath().normalize();
        return targetPath.startsWith(backendPath);
    }
}
