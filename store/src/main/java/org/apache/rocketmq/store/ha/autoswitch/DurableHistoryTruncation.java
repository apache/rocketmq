/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.rocksdb.RocksDBException;

/** Persist a destructive repair before removing the incident. */
final class DurableHistoryTruncation {
    private DurableHistoryTruncation() {
    }

    static void apply(DefaultMessageStore store, EpochFileCache epochs, HistoryIncident.Cut cut) throws IOException, RocksDBException {
        long offset = cut.offset;
        if (offset < store.getMinPhyOffset() || offset > store.getMaxPhyOffset()) {
            throw new IOException("Recovery offset outside retained commit log: " + offset);
        }
        if (!store.truncateFiles(offset) || store.getMaxPhyOffset() != offset) {
            throw new IOException("Could not truncate replica to " + offset);
        }
        // The ordinary truncate only moves in-memory positions inside the
        // retained segment. A later load scans its bytes, so even records
        // following the next append must be invalidated.
        MappedFile last = store.getCommitLog().getMappedFileQueue().getLastMappedFile();
        if (last != null) {
            FileChannel channel = last.getFileChannel();
            long position = offset - last.getFileFromOffset();
            ByteBuffer zeros = ByteBuffer.allocate(64 * 1024);
            while (position < last.getFileSize()) {
                zeros.clear();
                zeros.limit((int) Math.min(zeros.capacity(), last.getFileSize() - position));
                while (zeros.hasRemaining()) {
                    position += channel.write(zeros, position);
                }
            }
            channel.force(true);
        }
        Set<String> directories = new HashSet<>(Arrays.asList(store.getMessageStoreConfig().getStorePathCommitLog()
            .split(MixAll.MULTI_PATH_SPLITTER)));
        String readOnly = store.getMessageStoreConfig().getReadOnlyCommitLogStorePaths();
        if (readOnly != null && !readOnly.trim().isEmpty()) {
            directories.addAll(Arrays.asList(readOnly.split(MixAll.MULTI_PATH_SPLITTER)));
        }
        for (String name : directories) {
            Path directory = Paths.get(name.trim());
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                for (Path file : files) {
                    String segment = file.getFileName().toString();
                    if (segment.matches("[0-9]{20}")) {
                        long start = Long.parseLong(segment);
                        if (start > offset && start < cut.originalEnd) {
                            throw new IOException("Old commit log segment still present: " + file);
                        }
                    }
                }
            }
            HistoryIncident.forceDirectory(directory);
        }
        epochs.truncateForRecovery(offset, Paths.get(store.getMessageStoreConfig().getStorePathEpochFile()));
        store.setConfirmOffset(offset);
        store.getStoreCheckpoint().flushForRecovery();
    }
}
