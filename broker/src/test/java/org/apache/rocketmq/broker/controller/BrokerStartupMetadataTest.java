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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class BrokerStartupMetadataTest {
    @Rule
    public TemporaryFolder directory = new TemporaryFolder();

    @Test
    public void incrementsPersistedGenerationAcrossStarts() throws Exception {
        Path path = directory.getRoot().toPath().resolve("startup");
        assertEquals(1, BrokerStartupMetadata.nextGeneration(path));
        assertEquals(2, BrokerStartupMetadata.nextGeneration(path));
        assertEquals(3, BrokerStartupMetadata.nextGeneration(path));
    }

    @Test(expected = IOException.class)
    public void emptyMetadataDoesNotBecomeFirstStartup() throws Exception {
        BrokerStartupMetadata.nextGeneration(directory.newFile("startup").toPath());
    }

    @Test(expected = IOException.class)
    public void damagedMetadataDoesNotResetGeneration() throws Exception {
        Path path = directory.getRoot().toPath().resolve("startup");
        BrokerStartupMetadata.nextGeneration(path);
        Files.write(path, "1\n9\n0\n".getBytes(StandardCharsets.UTF_8));
        BrokerStartupMetadata.nextGeneration(path);
    }

    @Test(expected = IOException.class)
    public void refusesUnwritableMetadataTarget() throws Exception {
        BrokerStartupMetadata.nextGeneration(directory.newFolder("startup").toPath());
    }
}
