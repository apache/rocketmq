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

package org.apache.rocketmq.common.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class IOTinyUtilsRelativePathTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCopyToRelativeFileName() throws IOException {
        copyToRelativeFile(false);
    }

    @Test
    public void testOverwriteRelativeFileName() throws IOException {
        copyToRelativeFile(true);
    }

    @Test
    public void testNestedTargetDirectoriesAreStillCreated() throws IOException {
        Path source = temporaryFolder.newFile().toPath();
        byte[] content = "source-content".getBytes(StandardCharsets.UTF_8);
        Files.write(source, content);
        Path target = temporaryFolder.getRoot().toPath().resolve("nested/copy.bin");
        IOTinyUtils.copyFile(source.toString(), target.toString());
        assertArrayEquals(content, Files.readAllBytes(target));
    }

    @Test
    public void testMissingSourceDoesNotOverwriteExistingTarget() throws IOException {
        Path target = relativeTarget();
        byte[] original = "keep-existing".getBytes(StandardCharsets.UTF_8);
        try {
            Files.write(target, original);
            String missingSource = temporaryFolder.getRoot().toPath().resolve("missing").toString();
            assertThrows(IllegalArgumentException.class, () -> IOTinyUtils.copyFile(missingSource, target.toString()));
            assertArrayEquals(original, Files.readAllBytes(target));
        } finally {
            Files.deleteIfExists(target);
        }
    }

    private void copyToRelativeFile(boolean existing) throws IOException {
        Path source = temporaryFolder.newFile().toPath();
        byte[] content = "copied-content".getBytes(StandardCharsets.UTF_8);
        Files.write(source, content);
        Path target = relativeTarget();
        try {
            if (existing) {
                Files.write(target, "old-content-is-longer".getBytes(StandardCharsets.UTF_8));
            }
            IOTinyUtils.copyFile(source.toString(), target.toString());
            assertArrayEquals(content, Files.readAllBytes(target));
        } finally {
            Files.deleteIfExists(target);
        }
    }

    private Path relativeTarget() {
        return Paths.get("iotiny-copy-" + UUID.randomUUID() + ".tmp");
    }
}
