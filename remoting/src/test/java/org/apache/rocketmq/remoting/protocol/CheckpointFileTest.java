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

package org.apache.rocketmq.remoting.protocol;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.CheckpointFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointFileTest {

    private static final String FILE_PATH =
        Paths.get(System.getProperty("java.io.tmpdir"), "store-test", "epoch.ckpt").toString();

    private List<EpochEntry> entryList;
    private CheckpointFile<EpochEntry> checkpoint;

    static class EpochEntrySerializer implements CheckpointFile.CheckpointSerializer<EpochEntry> {

        @Override
        public String toLine(EpochEntry entry) {
            if (entry != null) {
                return String.format("%d-%d", entry.getEpoch(), entry.getStartOffset());
            } else {
                return null;
            }
        }

        @Override
        public EpochEntry fromLine(String line) {
            final String[] arr = line.split("-");
            if (arr.length == 2) {
                final int epoch = Integer.parseInt(arr[0]);
                final long startOffset = Long.parseLong(arr[1]);
                return new EpochEntry(epoch, startOffset);
            }
            return null;
        }
    }

    @Before
    public void init() throws IOException {
        this.entryList = new ArrayList<>();
        entryList.add(new EpochEntry(7, 7000));
        entryList.add(new EpochEntry(8, 8000));
        this.checkpoint = new CheckpointFile<>(FILE_PATH, new EpochEntrySerializer());
        this.checkpoint.write(entryList);
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(FILE_PATH));
        UtilAll.deleteFile(new File(FILE_PATH + ".bak"));
    }

    @Test
    public void testNormalWriteAndRead() throws IOException {
        List<EpochEntry> listFromFile = checkpoint.read();
        Assert.assertEquals(entryList, listFromFile);
        checkpoint.write(entryList);
        listFromFile = checkpoint.read();
        Assert.assertEquals(entryList, listFromFile);
    }

    @Test
    public void testAbNormalWriteAndRead() throws IOException {
        this.checkpoint.write(entryList);
        UtilAll.deleteFile(new File(FILE_PATH));
        List<EpochEntry> listFromFile = checkpoint.read();
        Assert.assertEquals(entryList, listFromFile);
        checkpoint.write(entryList);
        listFromFile = checkpoint.read();
        Assert.assertEquals(entryList, listFromFile);
    }
}
