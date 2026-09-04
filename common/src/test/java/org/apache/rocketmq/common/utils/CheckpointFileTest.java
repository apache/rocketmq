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

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckpointFileTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void writeEmptyEntriesClearsPreviousCheckpoint() throws Exception {
        File file = new File(temporaryFolder.getRoot(), "checkpoint");
        CheckpointFile<Integer> checkpointFile = newCheckpointFile(file);
        checkpointFile.write(Arrays.asList(1, 2));

        checkpointFile.write(Collections.emptyList());

        assertThat(checkpointFile.read()).isEmpty();
        assertThat(Files.readAllLines(file.toPath()).get(0)).isEqualTo("0");
    }

    @Test
    public void readFallsBackToBackupWhenPrimaryIsCorrupt() throws Exception {
        File file = new File(temporaryFolder.getRoot(), "checkpoint");
        CheckpointFile<Integer> checkpointFile = newCheckpointFile(file);
        checkpointFile.write(Collections.singletonList(1));
        checkpointFile.write(Collections.singletonList(2));
        Files.write(file.toPath(), Arrays.asList("1", "1", "2"));

        assertThat(checkpointFile.read()).containsExactly(1);
    }

    private CheckpointFile<Integer> newCheckpointFile(File file) {
        return new CheckpointFile<>(file.getAbsolutePath(), new CheckpointFile.CheckpointSerializer<Integer>() {
            @Override
            public String toLine(Integer entry) {
                return entry.toString();
            }

            @Override
            public Integer fromLine(String line) {
                return Integer.valueOf(line);
            }
        });
    }
}
