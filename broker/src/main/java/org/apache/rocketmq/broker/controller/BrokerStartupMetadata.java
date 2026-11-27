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
package org.apache.rocketmq.broker.controller;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;

/**
 * Advances once before registering a recovered store. A per-process startup ID, sent alongside the
 * generation, prevents a restored metadata file from being mistaken for a registration retry.
 */
final class BrokerStartupMetadata {
    private BrokerStartupMetadata() {
    }

    static long nextGeneration(Path file) throws IOException {
        final Path target = file.toAbsolutePath();
        long previous = 0;
        try {
            final String[] lines = new String(Files.readAllBytes(target), StandardCharsets.UTF_8).split("\n", -1);
            if (lines.length != 4 || !"1".equals(lines[0]) || !lines[3].isEmpty()) {
                throw new IOException("Invalid broker startup metadata: " + target);
            }
            final String payload = lines[0] + "\n" + lines[1] + "\n";
            if (Integer.parseInt(lines[2]) != UtilAll.crc32(payload.getBytes(StandardCharsets.UTF_8))) {
                throw new IOException("Broker startup metadata checksum mismatch: " + target);
            }
            previous = Long.parseLong(lines[1]);
            if (previous < 1 || previous == Long.MAX_VALUE) {
                throw new IOException("Invalid broker startup generation: " + previous);
            }
        } catch (NoSuchFileException ignored) {
            // First upgraded startup. The controller still checks its own persisted generation.
        } catch (NumberFormatException e) {
            throw new IOException("Invalid broker startup metadata: " + target, e);
        }

        final long next = previous + 1;
        final String payload = "1\n" + next + "\n";
        final byte[] data = (payload + UtilAll.crc32(payload.getBytes(StandardCharsets.UTF_8)) + "\n")
            .getBytes(StandardCharsets.UTF_8);
        final Path temporary = Files.createTempFile(target.getParent(), target.getFileName() + ".", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                final ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            MixAll.fsyncDirectory(target.getParent());
        } finally {
            Files.deleteIfExists(temporary);
        }
        return next;
    }
}
