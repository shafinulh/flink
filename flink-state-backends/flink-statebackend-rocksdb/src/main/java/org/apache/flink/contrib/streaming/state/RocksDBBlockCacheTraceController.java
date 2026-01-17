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
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.rocksdb.AbstractTraceWriter;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.TraceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/** Controls RocksDB block cache tracing lifecycle and owns the trace writer. */
class RocksDBBlockCacheTraceController implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBBlockCacheTraceController.class);

    private final RocksDB db;
    private final FileTraceWriter traceWriter;
    private final File traceFile;
    private boolean closed;

    private RocksDBBlockCacheTraceController(
            RocksDB db, FileTraceWriter traceWriter, File traceFile) {
        this.db = db;
        this.traceWriter = traceWriter;
        this.traceFile = traceFile;
    }

    @Nullable
    static RocksDBBlockCacheTraceController startTracing(
            RocksDB db,
            RocksDBBlockCacheTraceOptions options,
            String operatorIdentifier,
            @Nullable TaskInfo taskInfo,
            File instanceBasePath,
            KeyGroupRange keyGroupRange,
            @Nullable JobID jobId)
            throws IOException, RocksDBException {
        if (!options.isEnabled()) {
            return null;
        }

        final File targetDirectory = options.resolveOutputDirectory(instanceBasePath, jobId);
        Files.createDirectories(targetDirectory.toPath());

        final File traceFile =
                new File(
                        targetDirectory,
                        buildTraceFileName(operatorIdentifier, taskInfo, keyGroupRange));
        final FileTraceWriter traceWriter =
                FileTraceWriter.open(traceFile.toPath(), options.isSyncOnFlush());

        final TraceOptions traceOptions =
                options.getMaxFileSizeBytes() > 0
                        ? new TraceOptions(options.getMaxFileSizeBytes())
                        : new TraceOptions();
        boolean success = false;
        try {
            db.startBlockCacheTrace(traceOptions, traceWriter);
            success = true;
        } finally {
            if (!success) {
                traceWriter.closeQuietly();
            }
        }

        LOG.info(
                "Started RocksDB block cache trace for operator {} (task={}, key-groups={}..{}) at {}",
                operatorIdentifier,
                taskInfo != null ? taskInfo.getTaskNameWithSubtasks() : "unknown",
                keyGroupRange.getStartKeyGroup(),
                keyGroupRange.getEndKeyGroup(),
                traceFile);

        return new RocksDBBlockCacheTraceController(db, traceWriter, traceFile);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            db.endBlockCacheTrace();
        } catch (RocksDBException e) {
            LOG.warn("Failed to stop RocksDB block cache trace {}", traceFile, e);
        } finally {
            traceWriter.closeQuietly();
        }
        LOG.info("Stopped RocksDB block cache trace {}", traceFile);
    }

    private static String buildTraceFileName(
            String operatorIdentifier, @Nullable TaskInfo taskInfo, KeyGroupRange keyGroupRange) {
        final String sanitizedOperator = operatorIdentifier.replaceAll("[^a-zA-Z0-9\\-]", "_");
        final String taskComponent;
        if (taskInfo == null) {
            taskComponent = "subtask-unknown";
        } else {
            taskComponent =
                    String.format(
                            Locale.ROOT,
                            "subtask-%d_attempt-%d",
                            taskInfo.getIndexOfThisSubtask(),
                            taskInfo.getAttemptNumber());
        }
        return String.format(
                Locale.ROOT,
                "%s_%s_key-groups-%d-%d_%d.trace",
                sanitizedOperator,
                taskComponent,
                keyGroupRange.getStartKeyGroup(),
                keyGroupRange.getEndKeyGroup(),
                System.currentTimeMillis());
    }

    private static final class FileTraceWriter extends AbstractTraceWriter {

        private final Path targetPath;
        private final FileChannel channel;
        private final boolean syncOnFlush;
        private final AtomicLong bytesWritten;

        private FileTraceWriter(Path targetPath, FileChannel channel, boolean syncOnFlush) {
            this.targetPath = targetPath;
            this.channel = channel;
            this.syncOnFlush = syncOnFlush;
            this.bytesWritten = new AtomicLong();
        }

        static FileTraceWriter open(Path targetPath, boolean syncOnFlush) throws IOException {
            Files.createDirectories(targetPath.getParent());
            FileChannel channel =
                    FileChannel.open(
                            targetPath,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING);
            return new FileTraceWriter(targetPath, channel, syncOnFlush);
        }

        @Override
        public synchronized void write(Slice slice) throws RocksDBException {
            try {
                final byte[] data = slice.data();
                final ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                bytesWritten.addAndGet(data.length);
                if (syncOnFlush) {
                    channel.force(false);
                }
            } catch (IOException e) {
                throw new RocksDBException(
                        "Failed to persist RocksDB block cache trace to "
                                + targetPath
                                + ": "
                                + e.getMessage());
            }
        }

        @Override
        public synchronized void closeWriter() throws RocksDBException {
            try {
                channel.force(true);
                channel.close();
            } catch (IOException e) {
                throw new RocksDBException(
                        "Failed to close RocksDB block cache trace file "
                                + targetPath
                                + ": "
                                + e.getMessage());
            }
        }

        @Override
        public long getFileSize() {
            return bytesWritten.get();
        }

        void closeQuietly() {
            try {
                channel.close();
            } catch (IOException ignore) {
                // ignore
            }
        }
    }
}
