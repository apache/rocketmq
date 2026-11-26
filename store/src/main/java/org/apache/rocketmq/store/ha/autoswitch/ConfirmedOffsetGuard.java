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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.rocketmq.common.MixAll;

/**
 * Remembers a refused remote history across HA client and broker restarts. The observed offset is
 * diagnostic evidence, not a verified recovery boundary. This guard does not determine whether the
 * local replica is eligible to become master.
 */
public class ConfirmedOffsetGuard {
    private static final String VERSION = "rocketmq-confirmed-offset-guard-v1";
    private final Path stateFile;
    private boolean blocked;
    private boolean persisted;
    private String source;
    private int masterEpoch;
    private long observedOffset;
    private long confirmOffset;

    public ConfirmedOffsetGuard(Path stateFile) throws IOException {
        this.stateFile = stateFile.toAbsolutePath();
        if (this.stateFile.getFileName() == null) {
            throw new IOException("Invalid confirmed-offset guard path: " + stateFile);
        }
        final byte[] data;
        try {
            data = Files.readAllBytes(this.stateFile);
        } catch (NoSuchFileException e) {
            return;
        }
        load(data);
    }

    public synchronized boolean isBlocked() {
        return this.blocked;
    }

    public synchronized void recordRefusal(String source, int masterEpoch, long observedOffset,
        long confirmOffset) throws IOException {
        if (!this.blocked) {
            this.source = source == null ? "" : source;
            this.masterEpoch = masterEpoch;
            this.observedOffset = observedOffset;
            this.confirmOffset = confirmOffset;
            this.blocked = true;
        }
        ensurePersisted();
    }

    public synchronized void ensurePersisted() throws IOException {
        if (this.blocked && !this.persisted) {
            persist();
            this.persisted = true;
        }
    }

    public synchronized String describe() {
        if (!this.blocked) {
            return "No refused remote history";
        }
        return "source=" + this.source + ", masterEpoch=" + this.masterEpoch
            + ", observedOffset=" + this.observedOffset + ", confirmOffset=" + this.confirmOffset
            + ", persisted=" + this.persisted;
    }

    private void load(byte[] data) throws IOException {
        final String text = decodeUtf8(data);
        final String[] fields = text.split("\n", -1);
        if (fields.length != 7 || !VERSION.equals(fields[0]) || !fields[6].isEmpty()) {
            throw new IOException("Invalid confirmed-offset guard record: " + this.stateFile);
        }
        final String payload = String.join("\n", fields[0], fields[1], fields[2], fields[3], fields[4]) + "\n";
        try {
            final byte[] checksum = Base64.getDecoder().decode(fields[5]);
            if (!MessageDigest.isEqual(checksum, digest(payload.getBytes(StandardCharsets.UTF_8)))) {
                throw new IOException("Confirmed-offset guard checksum mismatch: " + this.stateFile);
            }
            this.source = decodeUtf8(Base64.getDecoder().decode(fields[1]));
            this.masterEpoch = Integer.parseInt(fields[2]);
            this.observedOffset = Long.parseLong(fields[3]);
            this.confirmOffset = Long.parseLong(fields[4]);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid confirmed-offset guard record: " + this.stateFile, e);
        }
        this.blocked = true;
        this.persisted = true;
    }

    private void persist() throws IOException {
        final String encodedSource = Base64.getEncoder().encodeToString(this.source.getBytes(StandardCharsets.UTF_8));
        final String payload = VERSION + "\n" + encodedSource + "\n" + this.masterEpoch + "\n"
            + this.observedOffset + "\n" + this.confirmOffset + "\n";
        final String checksum = Base64.getEncoder().encodeToString(digest(payload.getBytes(StandardCharsets.UTF_8)));
        final byte[] data = (payload + checksum + "\n").getBytes(StandardCharsets.UTF_8);
        final Path parent = this.stateFile.getParent();
        final Path temporaryFile = Files.createTempFile(parent, "confirmed-offset-guard-", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporaryFile, StandardOpenOption.WRITE)) {
                final ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporaryFile, this.stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
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

    private static String decodeUtf8(byte[] data) throws IOException {
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(data)).toString();
    }

    private static byte[] digest(byte[] data) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is unavailable", e);
        }
    }
}
