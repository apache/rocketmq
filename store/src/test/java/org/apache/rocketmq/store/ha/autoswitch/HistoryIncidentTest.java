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
        incident.begin();
        assertNull(incident.authorize(80, 100, 125, "a 1@0", true));
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
        incident.begin();
        assertNull(incident.authorize(80, 100, 125, "a 1@0", true));
        Files.write(approval, old);
        assertNull(incident.authorize(80, 100, 125, "a 1@0", false));
    }

    @Test
    public void interruptedProbeRequiresApprovalToDiscardEntireLog() throws IOException {
        Path root = temp.newFolder().toPath();
        new HistoryIncident(root).begin();
        HistoryIncident restarted = new HistoryIncident(root);
        assertNull(restarted.authorize(100, 100, 100, "a 1@0", false));
        Files.copy(root.resolve("ha-history-incident"), root.resolve("ha-history-approval"));
        assertEquals(0, restarted.authorize(100, 100, 100, "a 1@0", false).offset);
    }

    @Test
    public void invalidApprovalAndUnreadableStateFailClosed() throws IOException {
        Path root = temp.newFolder().toPath();
        HistoryIncident incident = new HistoryIncident(root);
        incident.begin();
        assertNull(incident.authorize(1, 2, 2, "a 1@0", true));
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
