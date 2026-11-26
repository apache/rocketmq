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

package org.apache.rocketmq.common.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;

/**
 * Entry Checkpoint file util
 * Format:
 * <li>First line:  Entries size
 * <li>Second line: Entries crc32
 * <li>Next: Entry data per line
 * <p>
 * Example:
 * <li>2 (size)
 * <li>773307083 (crc32)
 * <li>7-7000 (entry data)
 * <li>8-8000 (entry data)
 */
public class CheckpointFile<T> {

    /**
     * Not check crc32 when value is 0
     */
    private static final int NOT_CHECK_CRC_MAGIC_CODE = 0;
    private final String filePath;
    private final CheckpointSerializer<T> serializer;

    public interface CheckpointSerializer<T> {
        /**
         * Serialize entry to line
         */
        String toLine(final T entry);

        /**
         * DeSerialize line to entry
         */
        T fromLine(final String line);
    }

    public CheckpointFile(final String filePath, final CheckpointSerializer<T> serializer) {
        this.filePath = filePath;
        this.serializer = serializer;
    }

    public String getBackFilePath() {
        return this.filePath + ".bak";
    }

    /**
     * Write entries to file
     */
    public void write(final List<T> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        synchronized (this) {
            StringBuilder entryContent = new StringBuilder();
            for (T entry : entries) {
                final String line = this.serializer.toLine(entry);
                if (line != null && !line.isEmpty()) {
                    entryContent.append(line);
                    entryContent.append(System.lineSeparator());
                }
            }
            int crc32 = UtilAll.crc32(entryContent.toString().getBytes(StandardCharsets.UTF_8));

            String content = entries.size() + System.lineSeparator() +
                crc32 + System.lineSeparator() + entryContent;
            MixAll.string2File(content, this.filePath);
        }
    }

    private List<T> read(String filePath) throws IOException {
        final ArrayList<T> result = new ArrayList<>();
        synchronized (this) {
            final File file = new File(filePath);
            if (!file.exists()) {
                return result;
            }
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                // Read size
                int expectedLines = Integer.parseInt(reader.readLine());

                // Read block crc
                int expectedCrc32 = Integer.parseInt(reader.readLine());

                // Read entries
                StringBuilder sb = new StringBuilder();
                String line = reader.readLine();
                while (line != null) {
                    sb.append(line).append(System.lineSeparator());
                    final T entry = this.serializer.fromLine(line);
                    if (entry != null) {
                        result.add(entry);
                    }
                    line = reader.readLine();
                }
                int truthCrc32 = UtilAll.crc32(sb.toString().getBytes(StandardCharsets.UTF_8));

                if (result.size() != expectedLines) {
                    final String err = String.format(
                        "Expect %d entries, only found %d entries", expectedLines, result.size());
                    throw new IOException(err);
                }

                if (NOT_CHECK_CRC_MAGIC_CODE != expectedCrc32 && truthCrc32 != expectedCrc32) {
                    final String err = String.format(
                        "Entries crc32 not match, file=%s, expected=%s, truth=%s", filePath, expectedCrc32, truthCrc32);
                    throw new IOException(err);
                }
                return result;
            }
        }
    }

    /**
     * Read entries from file
     */
    public List<T> read() throws IOException {
        try {
            List<T> result = this.read(this.filePath);
            if (CollectionUtils.isEmpty(result)) {
                result = this.read(this.getBackFilePath());
            }
            return result;
        } catch (IOException e) {
            return this.read(this.getBackFilePath());
        }
    }

    /**
     * Read the primary checkpoint without falling back to an older backup. Callers that use the
     * checkpoint as a durable history boundary must not silently restore an earlier boundary.
     */
    public synchronized List<T> readStrict() throws IOException {
        final Path path = Paths.get(this.filePath);
        final List<T> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            final int expectedLines = Integer.parseInt(reader.readLine());
            final int expectedCrc32 = Integer.parseInt(reader.readLine());
            if (expectedLines < 0) {
                throw new IOException("Invalid checkpoint entry count: " + this.filePath);
            }
            final StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
                final T entry = this.serializer.fromLine(line);
                if (entry == null) {
                    throw new IOException("Invalid checkpoint entry: " + this.filePath);
                }
                result.add(entry);
            }
            if (result.size() != expectedLines) {
                throw new IOException("Checkpoint entry count does not match: " + this.filePath);
            }
            final int actualCrc32 = UtilAll.crc32(content.toString().getBytes(StandardCharsets.UTF_8));
            if (actualCrc32 != expectedCrc32) {
                throw new IOException("Checkpoint checksum does not match: " + this.filePath);
            }
            return result;
        } catch (NoSuchFileException e) {
            if (Files.exists(Paths.get(getBackFilePath()))) {
                throw new IOException("Primary checkpoint is missing while a backup exists: " + this.filePath, e);
            }
            return result;
        } catch (RuntimeException e) {
            throw new IOException("Invalid checkpoint: " + this.filePath, e);
        }
    }

    /**
     * Replace a checkpoint only after its complete contents have been forced to disk. Empty lists
     * are persisted as well, so a removed history cannot reappear from an earlier checkpoint.
     */
    public synchronized void writeAtomically(final List<T> entries) throws IOException {
        final StringBuilder entryContent = new StringBuilder();
        for (T entry : entries) {
            final String line = this.serializer.toLine(entry);
            if (line == null || line.isEmpty() || line.indexOf('\n') >= 0 || line.indexOf('\r') >= 0) {
                throw new IOException("Invalid checkpoint entry: " + this.filePath);
            }
            entryContent.append(line).append(System.lineSeparator());
        }
        final int crc32 = UtilAll.crc32(entryContent.toString().getBytes(StandardCharsets.UTF_8));
        final byte[] data = (entries.size() + System.lineSeparator() + crc32 + System.lineSeparator()
            + entryContent).getBytes(StandardCharsets.UTF_8);
        final Path path = Paths.get(this.filePath).toAbsolutePath();
        final Path parent = path.getParent();
        if (path.getFileName() == null || parent == null) {
            throw new IOException("Invalid checkpoint path: " + this.filePath);
        }
        Files.createDirectories(parent);
        final Path temporaryFile = Files.createTempFile(parent, "checkpoint-", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporaryFile, StandardOpenOption.WRITE)) {
                final ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporaryFile, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            MixAll.fsyncDirectory(parent);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(temporaryFile);
            } catch (IOException cleanupException) {
                e.addSuppressed(cleanupException);
            }
            throw e;
        }
    }
}
