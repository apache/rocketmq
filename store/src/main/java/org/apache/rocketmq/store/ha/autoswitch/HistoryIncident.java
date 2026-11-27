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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.LongSupplier;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.rocketmq.remoting.protocol.EpochEntry;

/** An operator approves one incident by installing an exact copy as ha-history-approval. */
final class HistoryIncident {
    private static final byte[] CHECKING = "HA history check in progress\n".getBytes(StandardCharsets.UTF_8);
    private final Path state;
    private final Path approval;
    private final Path inspecting;
    private final Path received;

    HistoryIncident(Path root) {
        this.state = root.resolve("ha-history-incident");
        this.approval = root.resolve("ha-history-approval");
        this.inspecting = root.resolve("ha-history-inspecting");
        this.received = root.resolve("ha-history-received");
    }

    boolean isolated() throws IOException {
        replayReceived();
        return read(state) != null || read(inspecting) != null;
    }

    private static byte[] read(Path path) throws IOException {
        try {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw new IOException("Expected regular HA history file at " + path);
            }
        } catch (NoSuchFileException absent) {
            // A missing or unmounted Store root is not a clean Store.
            Files.readAttributes(path.getParent(), BasicFileAttributes.class);
            return null;
        }
        return Files.readAllBytes(path);
    }

    boolean beginInspection() throws IOException {
        replayReceived();
        if (read(inspecting) != null) {
            // A failed force can leave the marker visible only in memory.
            // Retry the force without taking ownership of that inspection.
            forceDirectory(inspecting.getParent());
            return false;
        }
        if (read(state) == null) {
            return false;
        }
        // Keep the old incident available to operators, but make its approval
        // unusable after a crash until the new observation is accounted for.
        Files.createFile(inspecting);
        forceDirectory(inspecting.getParent());
        return true;
    }

    void finishInspection() throws IOException {
        if (Files.deleteIfExists(inspecting)) {
            forceDirectory(inspecting.getParent());
        }
    }

    static void forceDirectory(Path directory) throws IOException {
        if (File.separatorChar == '\\' && directory.getFileSystem() == FileSystems.getDefault()) {
            WindowsDirectorySync.force(directory);
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private byte[] decision(long offset, long confirm, long end, String source) {
        String payload = UUID.randomUUID() + "\n" + offset + "\n" + confirm + "\n" + end + "\n" + source + "\n";
        return (payload + DigestUtils.sha256Hex(payload) + "\n").getBytes(StandardCharsets.UTF_8);
    }

    private static void writeAndForce(FileChannel channel, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(true);
    }

    private void record(long offset, long confirm, long end, String source) throws IOException {
        Path temp = Files.createTempFile(state.getParent(), "ha-incident-", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                writeAndForce(channel, decision(offset, confirm, end, source));
            }
            Files.move(temp, state, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(state.getParent());
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    static String source(String peer, List<EpochEntry> epochs) {
        StringBuilder result = new StringBuilder(peer);
        for (EpochEntry entry : epochs) {
            result.append(' ').append(entry.getEpoch()).append('@').append(entry.getStartOffset());
        }
        return result.toString();
    }

    static final class Cut {
        final long offset;
        final long originalEnd;

        Cut(long offset, long originalEnd) {
            this.offset = offset;
            this.originalEnd = originalEnd;
        }
    }

    Cut firstDecision(long point, LongSupplier confirmedOffset, long end, String source) throws IOException {
        boolean refused = journalReceivedDecision(point, confirmedOffset, end, source);
        finishReceived(refused);
        return refused ? null : new Cut(point, end);
    }

    boolean journalReceivedDecision(long point, LongSupplier confirmedOffset, long end, String source) throws IOException {
        // Only complete responses reach here. Reserve and force the journal
        // entry before consulting confirmation to decide whether to reject.
        long confirm;
        try (FileChannel channel = FileChannel.open(received, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            forceDirectory(received.getParent());
            confirm = confirmedOffset.getAsLong();
            writeAndForce(channel, decision(point, confirm, end, source));
        }
        return point < confirm;
    }

    private void replayReceived() throws IOException {
        byte[] bytes = read(received);
        if (bytes != null) {
            String[] fields = checkedFields(received, bytes);
            finishReceived(Long.parseLong(fields[1]) < Long.parseLong(fields[2]));
        }
    }

    private void finishReceived(boolean refused) throws IOException {
        if (refused) {
            if (read(state) != null) {
                throw new IOException("Conflicting HA history incident and received decision");
            }
            Files.move(received, state, StandardCopyOption.ATOMIC_MOVE);
        } else {
            Files.delete(received);
        }
        forceDirectory(received.getParent());
    }

    private static String[] checkedFields(Path path, byte[] bytes) throws IOException {
        String[] fields = new String(bytes, StandardCharsets.UTF_8).split("\n", -1);
        if (fields.length != 7 || !fields[6].isEmpty()
            || !DigestUtils.sha256Hex(String.join("\n", Arrays.copyOf(fields, 5)) + "\n").equals(fields[5])) {
            throw new IOException("Invalid HA history record at " + path);
        }
        return fields;
    }

    Cut authorize(long point, long confirm, long end, String source, boolean ownsInspection) throws IOException {
        byte[] bytes = read(state);
        boolean interrupted = !ownsInspection && read(inspecting) != null;
        if (bytes == null) {
            throw new IOException("HA history incident disappeared");
        }
        if (Arrays.equals(bytes, CHECKING)) {
            // The last process may have observed a fork without recording
            // its boundary. No portion of the local history is assumed safe.
            record(0, confirm, end, source);
            return null;
        }
        String[] fields = checkedFields(state, bytes);
        long limit = Long.parseLong(fields[1]);
        long originalConfirm = Long.parseLong(fields[2]);
        long originalEnd = Long.parseLong(fields[3]);
        if (limit < 0 || limit > originalEnd) {
            throw new IOException("Invalid HA recovery offset " + limit);
        }
        if (interrupted || point < limit || end > originalEnd || !fields[4].equals(source)) {
            record(interrupted ? 0 : Math.min(limit, point), originalConfirm, Math.max(end, originalEnd), source);
            return null;
        }
        if (!Arrays.equals(bytes, read(approval))) {
            return null;
        }
        // Consume before any destructive work. A crash requires a fresh
        // decision and a new copy of the current incident.
        record(limit, originalConfirm, originalEnd, source);
        Files.delete(approval);
        forceDirectory(state.getParent());
        return new Cut(limit, originalEnd);
    }

    void clear() throws IOException {
        Files.delete(state);
        forceDirectory(state.getParent());
    }
}
