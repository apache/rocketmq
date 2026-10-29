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
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class IOTinyUtilsSameFileTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void copyFileToItselfDoesNotDestroySource() throws Exception {
        Path source = folder.newFile("source").toPath();
        assertCopyRejected(source, source);
    }

    @Test
    public void copyFileToHardLinkDoesNotDestroySource() throws Exception {
        Path source = folder.newFile("source").toPath();
        Path link = source.resolveSibling("link");
        try {
            Files.createLink(link, source);
        } catch (IOException | UnsupportedOperationException e) {
            Assume.assumeNoException("Hard links are unavailable on this filesystem", e);
        }
        assertCopyRejected(source, link);
    }

    @Test
    public void copyFileToDifferentFileStillWorks() throws Exception {
        Path source = folder.newFile("source").toPath();
        Path target = folder.newFile("target").toPath();
        byte[] data = "important data".getBytes(StandardCharsets.UTF_8);
        Files.write(source, data);
        IOTinyUtils.copyFile(source.toString(), target.toString());
        assertArrayEquals(data, Files.readAllBytes(target));
    }

    private void assertCopyRejected(Path source, Path target) throws Exception {
        byte[] data = "important data".getBytes(StandardCharsets.UTF_8);
        Files.write(source, data);
        try {
            IOTinyUtils.copyFile(source.toString(), target.toString());
            assertArrayEquals(data, Files.readAllBytes(source));
            fail("Expected an IOException for identical files");
        } catch (IOException expected) {
            assertArrayEquals(data, Files.readAllBytes(source));
        }
    }
}
