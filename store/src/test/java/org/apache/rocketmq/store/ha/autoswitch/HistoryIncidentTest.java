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
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HistoryIncidentTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void approvalBindsEndpointEpochsCutAndNonce() throws IOException {
        Path root = temp.newFolder().toPath();
        HistoryIncident incident = new HistoryIncident(root);
        Path state = root.resolve("ha-history-incident");
        Path approval = root.resolve("ha-history-approval");
        assertNull(incident.firstDecision(80, () -> 100, 125, "a 1@0"));
        byte[] old = Files.readAllBytes(state);
        Files.write(approval, old);
        assertNull(incident.authorize(80, 100, 125, "b 1@0", false));
        assertNull(incident.authorize(80, 100, 125, "a 1@0", false));
        Files.write(approval, Files.readAllBytes(state));
        assertNull(incident.authorize(80, 100, 125, "a 1@0 2@80", false));
        Files.write(approval, Files.readAllBytes(state));
        assertNull(incident.authorize(60, 100, 125, "a 1@0 2@80", false));
        assertNull(incident.authorize(100, 100, 125, "a 1@0 2@80", false));
        byte[] accepted = Files.readAllBytes(state);
        Files.write(approval, accepted);
        HistoryIncident.Cut cut = incident.authorize(100, 100, 125, "a 1@0 2@80", false);
        assertEquals(60, cut.offset);
        assertEquals(125, cut.originalEnd);
        assertFalse(Files.exists(approval));
        // Crash after consuming authorization and before completing repair.
        incident = new HistoryIncident(root);
        assertTrue(incident.isolated());
        Files.write(approval, accepted);
        assertNull(incident.authorize(60, 60, 60, "a 1@0 2@80", false));
        Files.write(approval, Files.readAllBytes(state));
        assertEquals(60, incident.authorize(60, 60, 60, "a 1@0 2@80", false).offset);
        incident.clear();
        assertNull(incident.firstDecision(80, () -> 100, 125, "a 1@0"));
        Files.write(approval, old);
        assertNull(incident.authorize(80, 100, 125, "a 1@0", false));
    }

    @Test
    public void healthyWaitHasNoIncidentButInterruptedIsolatedInspectionInvalidatesApproval() throws IOException {
        Path root = temp.newFolder().toPath();
        HistoryIncident original = new HistoryIncident(root);
        assertFalse(original.beginInspection());
        HistoryIncident restarted = new HistoryIncident(root);
        assertEquals(100, restarted.firstDecision(100, () -> 100, 100, "a 1@0").offset);
        assertNull(restarted.firstDecision(80, () -> 100, 100, "a 1@0"));
        byte[] stale = Files.readAllBytes(root.resolve("ha-history-incident"));
        Files.write(root.resolve("ha-history-approval"), stale);
        assertTrue(restarted.beginInspection());
        restarted = new HistoryIncident(root);
        assertFalse(restarted.beginInspection());
        assertNull(restarted.authorize(100, 100, 100, "a 1@0", false));
        restarted.finishInspection();
        Files.write(root.resolve("ha-history-approval"), stale);
        assertNull(restarted.authorize(100, 100, 100, "a 1@0", false));
        Files.write(root.resolve("ha-history-approval"), Files.readAllBytes(root.resolve("ha-history-incident")));
        assertEquals(0, restarted.authorize(100, 100, 100, "a 1@0", false).offset);
    }

    @Test
    public void restartReplaysReceivedDecisionAfterPublicationFails() throws IOException {
        for (long point : new long[] {60, 100}) {
            Path root = temp.newFolder().toPath();
            Path offline = root.resolveSibling(root.getFileName() + "-offline");
            HistoryIncident incident = new HistoryIncident(root);
            try {
                assertThatThrownBy(() -> incident.firstDecision(point, () -> {
                    try {
                        Files.move(root, offline);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return 100;
                }, 110, "a 1@0")).isInstanceOf(IOException.class);
            } finally {
                Files.move(offline, root);
            }
            HistoryIncident restarted = new HistoryIncident(root);
            assertEquals(point < 100, restarted.isolated());
            if (point < 100) {
                assertNull(restarted.authorize(110, 100, 110, "a 1@0", false));
                Files.copy(root.resolve("ha-history-incident"), root.resolve("ha-history-approval"));
                assertEquals(point, restarted.authorize(110, 100, 110, "a 1@0", false).offset);
            }
        }
    }

    @Test
    public void retryMustForceAlreadyVisibleInspectionMarker() throws IOException {
        Path archive = temp.newFolder().toPath().resolve("store.zip");
        // ZipFS permits creating and reading a file but cannot open its
        // directory as a FileChannel for force(true).
        try (FileSystem fs = FileSystems.newFileSystem(URI.create("jar:" + archive.toUri()),
            Collections.singletonMap("create", "true"))) {
            Path root = Files.createDirectory(fs.getPath("/store"));
            Files.createFile(root.resolve("ha-history-incident"));
            assertThatThrownBy(new HistoryIncident(root)::beginInspection).isInstanceOf(IOException.class);
            assertTrue(Files.exists(root.resolve("ha-history-inspecting")));
            assertThatThrownBy(new HistoryIncident(root)::beginInspection).isInstanceOf(IOException.class);
        }
    }

    @Test
    public void invalidApprovalAndUnreadableStateFailClosed() throws IOException {
        Path root = temp.newFolder().toPath();
        HistoryIncident incident = new HistoryIncident(root);
        assertNull(incident.firstDecision(1, () -> 2, 2, "a 1@0"));
        Path approval = root.resolve("ha-history-approval");
        Files.write(approval, "bad".getBytes(StandardCharsets.UTF_8));
        assertNull(incident.authorize(1, 2, 2, "a 1@0", false));
        Files.delete(approval);
        Files.createDirectory(approval);
        assertThatThrownBy(() -> incident.authorize(1, 2, 2, "a 1@0", false)).isInstanceOf(IOException.class);
        assertTrue(incident.isolated());
        Path state = root.resolve("ha-history-incident");
        Files.delete(state);
        Files.createDirectory(state);
        assertThatThrownBy(incident::isolated).isInstanceOf(IOException.class);
    }
}
