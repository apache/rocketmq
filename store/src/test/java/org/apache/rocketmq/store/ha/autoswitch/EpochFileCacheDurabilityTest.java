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
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.utils.CheckpointFile;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EpochFileCacheDurabilityTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testRecoveredBoundarySurvivesGrowthAndReloadBeforeFirstHandshake() throws Exception {
        final Path path = epochPath();
        final EpochFileCache master = new EpochFileCache(path.toString());
        master.prepareMasterEpoch(1, 0);
        master.prepareMasterEpoch(2, 1580);

        final EpochFileCache follower = new EpochFileCache();
        follower.initCacheFromEntries(Collections.singletonList(new EpochEntry(1, 0, 3280)));

        // The follower first connects only after the restarted master has written past its old end.
        master.setLastEpochEntryEndOffset(4190);
        assertEquals(1580, follower.findConsistentPoint(master));
        assertEquals(1580, master.prepareMasterEpoch(2, 4190).getStartOffset());

        final EpochFileCache reloaded = reload(path);
        reloaded.setLastEpochEntryEndOffset(4190);
        assertEquals(1580, follower.findConsistentPoint(reloaded));
        assertEquals(1580, reloaded.getEntry(1).getEndOffset());
    }

    @Test
    public void testNewEpochReplacesAllLostAndEmptyTailEntries() throws Exception {
        final Path path = epochPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.initCacheFromEntries(Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 100),
            new EpochEntry(3, 200)));

        cache.prepareMasterEpoch(4, 100);

        final EpochFileCache reloaded = reload(path);
        assertEquals(2, reloaded.getEntrySize());
        assertEquals(100, reloaded.getEntry(1).getEndOffset());
        assertNull(reloaded.getEntry(2));
        assertNull(reloaded.getEntry(3));
        assertEquals(new EpochEntry(4, 100), reloaded.lastEntry());
    }

    @Test
    public void testRepeatedZeroWriteStartupsAndRecoveryToZero() throws Exception {
        final Path path = epochPath();
        EpochFileCache cache = new EpochFileCache(path.toString());
        cache.prepareMasterEpoch(1, 0);
        cache.prepareMasterEpoch(2, 100);
        cache.prepareMasterEpoch(3, 0);
        cache = reload(path);
        assertEquals(Collections.singletonList(new EpochEntry(3, 0)), cache.getAllEntries());

        cache.prepareMasterEpoch(4, 0);
        cache.prepareMasterEpoch(4, 0);
        assertEquals(Collections.singletonList(new EpochEntry(4, 0)), reload(path).getAllEntries());
    }

    @Test
    public void testStaleMasterGrantCannotChangeTheHistory() throws Exception {
        final Path path = epochPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.prepareMasterEpoch(3, 0);
        try {
            cache.prepareMasterEpoch(2, 100);
            fail("A stale master epoch must be rejected");
        } catch (IOException expected) {
            assertEquals(Collections.singletonList(new EpochEntry(3, 0)), reload(path).getAllEntries());
        }
    }

    @Test
    public void testFailedPersistenceDoesNotPublishAppendedOrTruncatedHistory() throws Exception {
        final Path path = epochPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.initCacheFromEntries(Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 100)));
        final List<EpochEntry> original = cache.getAllEntries();

        // Replacing a directory with a file fails even when the tests run with privileged access.
        Files.delete(path);
        Files.createDirectory(path);
        assertFalse(cache.appendEntry(new EpochEntry(3, 200)));
        assertEquals(original, cache.getAllEntries());
        assertFalse(cache.truncateSuffixByOffset(100));
        assertEquals(original, cache.getAllEntries());
        assertFalse(cache.truncateSuffixByEpoch(2));
        assertEquals(original, cache.getAllEntries());
        assertFalse(cache.truncatePrefixByOffset(100));
        assertEquals(original, cache.getAllEntries());

        for (int epoch : new int[] {2, 3}) {
            try {
                cache.prepareMasterEpoch(epoch, 200);
                fail("A master grant cannot succeed without persisting its boundary");
            } catch (IOException expected) {
                assertEquals(original, cache.getAllEntries());
            }
        }
    }

    @Test
    public void testPersistedEmptyHistoryDoesNotRestoreAnOlderBackup() throws Exception {
        final Path path = epochPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.prepareMasterEpoch(1, 0);
        Files.copy(path, path.resolveSibling(path.getFileName() + ".bak"));

        assertTrue(cache.truncateSuffixByOffset(0));
        assertEquals(0, reload(path).getEntrySize());

        Files.delete(path);
        assertFalse(new EpochFileCache(path.toString()).initCacheFromFile());
    }

    @Test
    public void testCorruptPrimaryDoesNotRestoreAnOlderBackup() throws Exception {
        final Path path = epochPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.prepareMasterEpoch(1, 0);
        Files.copy(path, path.resolveSibling(path.getFileName() + ".bak"));
        Files.write(path, "1\n".getBytes(StandardCharsets.UTF_8));

        assertFalse(cache.initCacheFromFile());
        assertEquals(Collections.singletonList(new EpochEntry(1, 0)), cache.getAllEntries());
        assertFalse(new EpochFileCache(path.toString()).initCacheFromFile());
    }

    @Test
    public void testValidChecksumDoesNotAllowInvalidEpochOrdering() throws Exception {
        final Path path = epochPath();
        final CheckpointFile<EpochEntry> checkpoint = new CheckpointFile<>(path.toString(),
            new EpochFileCache.EpochEntrySerializer());
        final List<List<EpochEntry>> invalidHistories = Arrays.asList(
            Arrays.asList(new EpochEntry(1, 0), new EpochEntry(1, 100)),
            Arrays.asList(new EpochEntry(2, 0), new EpochEntry(1, 100)),
            Arrays.asList(new EpochEntry(1, 100), new EpochEntry(2, 100)),
            Arrays.asList(new EpochEntry(1, 100), new EpochEntry(2, 0)),
            Collections.singletonList(new EpochEntry(1, -1)));
        for (List<EpochEntry> entries : invalidHistories) {
            checkpoint.writeAtomically(entries);
            assertFalse(new EpochFileCache(path.toString()).initCacheFromFile());
        }
    }

    @Test
    public void testZeroChecksumCannotDisableHistoryValidation() throws Exception {
        final Path path = epochPath();
        Files.write(path, ("1" + System.lineSeparator() + "0" + System.lineSeparator()
            + "1-0" + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        assertFalse(new EpochFileCache(path.toString()).initCacheFromFile());
    }

    @Test
    public void testMissingHistoryIsAcceptedForANewStore() {
        final EpochFileCache cache = new EpochFileCache(epochPath().toString());
        assertTrue(cache.initCacheFromFile());
        assertEquals(0, cache.getEntrySize());
    }

    private Path epochPath() {
        return temporaryFolder.getRoot().toPath().resolve("epoch");
    }

    private EpochFileCache reload(Path path) {
        final EpochFileCache cache = new EpochFileCache(path.toString());
        assertTrue(cache.initCacheFromFile());
        return cache;
    }
}
