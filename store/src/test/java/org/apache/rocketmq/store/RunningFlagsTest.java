/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RunningFlagsTest {

    @Test
    public void testNoFlagIsRenderedAsNone() {
        RunningFlags runningFlags = new RunningFlags();
        assertEquals(0, runningFlags.getFlagBits());
        assertEquals("NONE", runningFlags.getFlagBitsString());
        assertTrue(runningFlags.isWriteable());
        assertTrue(runningFlags.isReadable());
    }

    @Test
    public void testDiskFullIsRenderedAsName() {
        RunningFlags runningFlags = new RunningFlags();
        assertTrue(runningFlags.getAndMakeDiskFull());
        assertEquals(1 << 4, runningFlags.getFlagBits());
        assertEquals("DISK_FULL", runningFlags.getFlagBitsString());
        assertFalse(runningFlags.isWriteable());
    }

    @Test
    public void testMultipleFlagsAreJoined() {
        RunningFlags runningFlags = new RunningFlags();
        runningFlags.getAndMakeDiskFull();
        runningFlags.getAndMakeNotReadable();
        assertEquals("NOT_READABLE|DISK_FULL", runningFlags.getFlagBitsString());
    }

    @Test
    public void testDiskFullFlagCanBeCleared() {
        RunningFlags runningFlags = new RunningFlags();
        runningFlags.getAndMakeDiskFull();
        assertFalse(runningFlags.getAndMakeDiskOK());
        assertEquals("NONE", runningFlags.getFlagBitsString());
        assertTrue(runningFlags.isWriteable());
    }
}
