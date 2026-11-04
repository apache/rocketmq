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
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CheckpointFileHeaderTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final CheckpointFile.CheckpointSerializer<String> serializer =
        new CheckpointFile.CheckpointSerializer<String>() {
            @Override
            public String toLine(String entry) {
                return entry;
            }

            @Override
            public String fromLine(String line) {
                return line;
            }
        };

    @Test
    public void testTruncatedHeadersFallBackToValidBackup() throws IOException {
        for (String header : new String[] {"", "1", "1\n", "1\n\nvalue\n"}) {
            assertEquals(Collections.singletonList("saved"), withCorruptedPrimary(header).read());
        }
    }

    @Test
    public void testMalformedCountFallsBackToValidBackup() throws IOException {
        assertEquals(Collections.singletonList("saved"), withCorruptedPrimary("invalid\n0\nvalue\n").read());
    }

    @Test
    public void testMalformedCrcFallsBackToValidBackup() throws IOException {
        assertEquals(Collections.singletonList("saved"), withCorruptedPrimary("1\ninvalid\nvalue\n").read());
    }

    @Test
    public void testInvalidPrimaryAndBackupReportIOException() throws IOException {
        CheckpointFile<String> checkpoint = withCorruptedPrimary("invalid\n0\nvalue\n");
        Files.write(Paths.get(checkpoint.getBackFilePath()), "invalid".getBytes(StandardCharsets.UTF_8));
        IOException failure = assertThrows(IOException.class, checkpoint::read);
        assertTrue(failure.getCause() instanceof NumberFormatException);
        assertTrue(failure.getMessage().contains(checkpoint.getBackFilePath()));
    }

    @Test
    public void testCountAndCrcMismatchStillUseBackup() throws IOException {
        for (String content : new String[] {"2\n0\nvalue\n", "1\n1\nvalue\n"}) {
            assertEquals(Collections.singletonList("saved"), withCorruptedPrimary(content).read());
        }
    }

    @Test
    public void testValidPrimaryStillTakesPrecedence() throws IOException {
        CheckpointFile<String> checkpoint = withCorruptedPrimary("1\n0\ncurrent" + System.lineSeparator());
        assertEquals(Collections.singletonList("current"), checkpoint.read());
    }

    private CheckpointFile<String> withCorruptedPrimary(String content) throws IOException {
        Path path = temporaryFolder.newFile().toPath();
        CheckpointFile<String> checkpoint = new CheckpointFile<>(path.toString(), serializer);
        checkpoint.write(Collections.singletonList("saved"));
        Files.copy(path, Paths.get(checkpoint.getBackFilePath()), StandardCopyOption.REPLACE_EXISTING);
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        return checkpoint;
    }
}
