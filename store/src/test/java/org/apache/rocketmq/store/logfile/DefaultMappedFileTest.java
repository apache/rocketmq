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
package org.apache.rocketmq.store.logfile;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultMappedFileTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    String path;

    @Before
    public void setUp() throws IOException  {
        path = tmpFolder.newFolder("compaction").getAbsolutePath();
    }

    @Test
    public void testWriteFile() throws IOException  {
        Files.write(Paths.get(path,"test.file"), "111".getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Files.write(Paths.get(path,"test.file"), "111".getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        List<String> positions = Files.readAllLines(Paths.get(path, "test.file"), StandardCharsets.UTF_8);
        int p = Integer.parseInt(positions.stream().findFirst().orElse("0"));
        assertEquals(111, p);

        Files.write(Paths.get(path,"test.file"), "222".getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        positions = Files.readAllLines(Paths.get(path,"test.file"), StandardCharsets.UTF_8);
        p = Integer.parseInt(positions.stream().findFirst().orElse("0"));
        assertEquals(222, p);
    }

}