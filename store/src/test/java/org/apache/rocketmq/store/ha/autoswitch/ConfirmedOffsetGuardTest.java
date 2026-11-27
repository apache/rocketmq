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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfirmedOffsetGuardTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testMissingRecordDoesNotBlock() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);

        assertFalse(guard.isBlocked());
        assertFalse(Files.exists(stateFile));
    }

    @Test
    public void testRefusalSurvivesRestart() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        guard.recordRefusal("master-\u00e9:10912", 1, 1580, 3280);

        final ConfirmedOffsetGuard restored = new ConfirmedOffsetGuard(stateFile);
        assertTrue(guard.isBlocked());
        assertTrue(restored.isBlocked());
        assertEquals(guard.describe(), restored.describe());
        assertTrue(restored.describe().contains("observedOffset=1580"));
        assertTrue(restored.describe().contains("confirmOffset=3280"));
    }

    @Test
    public void testLaterRefusalKeepsFirstEvidence() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        guard.recordRefusal("first-master:10912", 1, 1580, 3280);
        final byte[] originalRecord = Files.readAllBytes(stateFile);
        final String originalDescription = guard.describe();

        guard.recordRefusal("new-master:10912", 2, 4190, 5000);
        assertEquals(originalDescription, guard.describe());
        assertArrayEquals(originalRecord, Files.readAllBytes(stateFile));

        final ConfirmedOffsetGuard restored = new ConfirmedOffsetGuard(stateFile);
        restored.recordRefusal("another-master:10912", 3, 6000, 7000);
        assertEquals(originalDescription, restored.describe());
        assertArrayEquals(originalRecord, Files.readAllBytes(stateFile));
    }

    @Test
    public void testEmptyRecordIsRejected() throws Exception {
        final Path stateFile = Files.createFile(stateFile());
        assertInvalidRecord(stateFile);
    }

    @Test
    public void testCorruptedRecordIsRejected() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        guard.recordRefusal("master:10912", 1, 1580, 3280);

        final String record = new String(Files.readAllBytes(stateFile), StandardCharsets.UTF_8);
        Files.write(stateFile, record.replace("1580", "1581").getBytes(StandardCharsets.UTF_8));
        assertInvalidRecord(stateFile);
    }

    @Test
    public void testTruncatedRecordIsRejected() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        guard.recordRefusal("master:10912", 1, 1580, 3280);

        final byte[] record = Files.readAllBytes(stateFile);
        Files.write(stateFile, Arrays.copyOf(record, record.length - 5));
        assertInvalidRecord(stateFile);
    }

    @Test
    public void testUnknownVersionIsRejected() throws Exception {
        final Path stateFile = stateFile();
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        guard.recordRefusal("master:10912", 1, 1580, 3280);

        final String record = new String(Files.readAllBytes(stateFile), StandardCharsets.UTF_8);
        Files.write(stateFile, record.replace("guard-v1", "guard-v2").getBytes(StandardCharsets.UTF_8));
        assertInvalidRecord(stateFile);
    }

    @Test
    public void testFailedWriteKeepsFirstRefusalAndCanRetry() throws Exception {
        final Path parent = tempFolder.getRoot().toPath().resolve("parent");
        final Path stateFile = parent.resolve("guard");
        final ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(stateFile);
        Files.createFile(parent);

        try {
            guard.recordRefusal("first-master:10912", 1, 1580, 3280);
            fail("Writing below a regular file must fail");
        } catch (IOException expected) {
            assertTrue(guard.isBlocked());
        }
        assertTrue(guard.describe().contains("persisted=false"));

        Files.delete(parent);
        Files.createDirectory(parent);
        guard.recordRefusal("new-master:10912", 2, 4190, 5000);

        final ConfirmedOffsetGuard restored = new ConfirmedOffsetGuard(stateFile);
        assertTrue(restored.isBlocked());
        assertEquals(guard.describe(), restored.describe());
        assertTrue(restored.describe().contains("source=first-master:10912"));
        assertTrue(restored.describe().contains("observedOffset=1580"));
        assertTrue(restored.describe().contains("confirmOffset=3280"));
    }

    @Test
    public void testDirectoryIsNotTreatedAsMissingRecord() throws Exception {
        final Path stateFile = Files.createDirectory(stateFile());
        assertInvalidRecord(stateFile);
    }

    private Path stateFile() {
        return tempFolder.getRoot().toPath().resolve("confirmed-offset-guard");
    }

    private void assertInvalidRecord(Path stateFile) throws Exception {
        try {
            new ConfirmedOffsetGuard(stateFile);
            fail("The invalid guard record must prevent loading");
        } catch (IOException expected) {
            assertTrue(Files.exists(stateFile));
        }
    }
}
