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
package org.apache.rocketmq.controller.impl.statemachine;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.impl.manager.MetadataManagerType;
import org.apache.rocketmq.controller.impl.manager.SnapshotAbleMetadataManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatemachineSnapshotFileGenerator {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    static class SnapshotFileHeader {
        // Magic + Version + TotalSections + BodyLength + Reversed
        public static final int HEADER_LENGTH = 18;
        public static final Integer MAGIC = -626843481;
        public static final Short VERSION = 1;
        public static final Long REVERSED = 0L;
        public final int totalSections;


        public SnapshotFileHeader(int totalSections) {
            this.totalSections = totalSections;
        }

        public static SnapshotFileHeader from(ByteBuffer header) {
            if (header == null || header.capacity() < HEADER_LENGTH) {
                return null;
            }
            int magic = header.getInt();
            if (magic != SnapshotFileHeader.MAGIC) {
                return null;
            }
            short version = header.getShort();
            if (version != SnapshotFileHeader.VERSION) {
                return null;
            }

            int totalSections = header.getInt();
            return new SnapshotFileHeader(totalSections);
        }

        public ByteBuffer build() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);
            buffer.putInt(MAGIC);
            buffer.putShort(VERSION);
            buffer.putInt(this.totalSections);
            buffer.putLong(REVERSED);
            buffer.flip();
            return buffer;
        }
    }

    private final Map<Short/*MetadataManagerId*/, SnapshotAbleMetadataManager> metadataManagerTable;

    public StatemachineSnapshotFileGenerator(final List<SnapshotAbleMetadataManager> managers) {
        this.metadataManagerTable = new HashMap<>();
        managers.forEach(manager -> this.metadataManagerTable.put(manager.getMetadataManagerType().getId(), manager));
    }


    /**
     * Generate snapshot and write the data to snapshot file.
     */
    public synchronized void generateSnapshot(final String snapshotPath) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(Paths.get(snapshotPath),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            // Write Snapshot Header
            SnapshotFileHeader header = new SnapshotFileHeader(this.metadataManagerTable.size());

            fileChannel.write(header.build());

            // Write each section
            ByteBuffer sectionHeader = ByteBuffer.allocate(6);
            for (Map.Entry<Short, SnapshotAbleMetadataManager> section : this.metadataManagerTable.entrySet()) {
                byte[] serializedBytes = section.getValue().encodeMetadata();
                // Section format: <Section-MetadataManagerType><Section-Length><Section-Bytes>

                // Write section header
                sectionHeader.putShort(section.getKey());
                sectionHeader.putInt(serializedBytes.length);
                sectionHeader.flip();
                fileChannel.write(sectionHeader);
                sectionHeader.rewind();

                // Write section bytes
                fileChannel.write(ByteBuffer.wrap(serializedBytes));
            }

            fileChannel.force(true);
        }
    }

    /**
     * Read snapshot from snapshot file and load the metadata into corresponding metadataManager
     */
    public synchronized boolean loadSnapshot(final String snapshotPath) throws IOException {
        try (ReadableByteChannel channel = Channels.newChannel(Files.newInputStream(Paths.get(snapshotPath)))) {
            // Read snapshot Header
            ByteBuffer header = ByteBuffer.allocate(SnapshotFileHeader.HEADER_LENGTH);
            if (channel.read(header) < 0) {
                return false;
            }
            header.rewind();

            SnapshotFileHeader fileHeader = SnapshotFileHeader.from(header);
            if (fileHeader == null) {
                return false;
            }

            // Read each section
            ByteBuffer sectionHeader = ByteBuffer.allocate(6);
            int successLoadCnt = 0;
            int readSize;
            while ((readSize = channel.read(sectionHeader)) > 0) {
                sectionHeader.rewind();

                if (readSize != sectionHeader.capacity()) {
                    throw new IOException("Invalid amount of data read for the header of a section");
                }

                // Section format: <Section-MetadataManagerType><Section-Length><Section-Bytes>
                short sectionType = sectionHeader.getShort();
                int length = sectionHeader.getInt();

                ByteBuffer data = ByteBuffer.allocate(length);
                readSize = channel.read(data);

                if (readSize != length) {
                    throw new IOException("Invalid amount of data read for the body of a section");
                }

                if (this.metadataManagerTable.containsKey(sectionType)) {
                    SnapshotAbleMetadataManager metadataManager = this.metadataManagerTable.get(sectionType);
                    if (!metadataManager.loadMetadata(data.array())) {
                        return false;
                    }
                    successLoadCnt ++;
                    log.info("Load snapshot metadata for {} success!", MetadataManagerType.from(sectionType));
                }
            }
            if (successLoadCnt != this.metadataManagerTable.size()) {
                log.info("Failed to load snapshot metadata file totally, expected section nums:{}, success load nums:{}", this.metadataManagerTable.size(), successLoadCnt);
                return false;
            }
        }
        return true;
    }
}
