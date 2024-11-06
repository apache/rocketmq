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

package org.apache.rocketmq.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;


public class MultiPathMappedFileQueueTest {

    @Test
    public void testGetLastMappedFile() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
            int idx = i % storePaths.length;
            assertThat(mappedFile.getFileName().startsWith(storePaths[idx])).isTrue();
        }
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testLoadReadOnlyMappedFiles() {
        {
            //create old mapped files
            final byte[] fixedMsg = new byte[1024];
            MessageStoreConfig config = new MessageStoreConfig();
            config.setStorePathCommitLog("target/unit_test_store/a/" + MixAll.MULTI_PATH_SPLITTER
                    + "target/unit_test_store/b/" + MixAll.MULTI_PATH_SPLITTER
                    + "target/unit_test_store/c/");
            MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
            String[] storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
            for (int i = 0; i < 1024; i++) {
                MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
                assertThat(mappedFile).isNotNull();
                assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
                int idx = i % storePaths.length;
                assertThat(mappedFile.getFileName().startsWith(storePaths[idx])).isTrue();
            }
            mappedFileQueue.shutdown(1000);
        }

        // test load and readonly
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/b/");
        config.setReadOnlyCommitLogStorePaths("target/unit_test_store/a" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c");
        MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);

        mappedFileQueue.load();

        assertThat(mappedFileQueue.mappedFiles.size()).isEqualTo(1024);
        for (int i = 0; i < 1024; i++) {
            assertThat(mappedFileQueue.mappedFiles.get(i).getFile().getName())
                    .isEqualTo(UtilAll.offset2FileName(1024 * i));
        }
        mappedFileQueue.destroy();

    }

    @Test
    public void testUpdatePathsOnline() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
            int idx = i % storePaths.length;
            assertThat(mappedFile.getFileName().startsWith(storePaths[idx])).isTrue();

            if (i == 500) {
                config.setStorePathCommitLog("target/unit_test_store/a/" + MixAll.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/b/");
                storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
            }
        }
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testFullStorePath() {
        final byte[] fixedMsg = new byte[1024];

        Set<String> fullStorePath = new HashSet<>();
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, () -> fullStorePath);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
        assertThat(storePaths.length).isEqualTo(3);

        MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
        assertThat(mappedFile.getFileName().startsWith(storePaths[0])).isTrue();

        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length);
        assertThat(mappedFile.getFileName().startsWith(storePaths[1])).isTrue();
        assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 2);
        assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
        assertThat(mappedFile.getFileName().startsWith(storePaths[2])).isTrue();

        fullStorePath.add("target/unit_test_store/b/");
        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 3);
        assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
        assertThat(mappedFile.getFileName().startsWith(storePaths[2])).isTrue();

        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 4);
        assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
        assertThat(mappedFile.getFileName().startsWith(storePaths[0])).isTrue();

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testUniqueNextNextMappedFile() throws IOException {
        Set<String> fullStorePath = new HashSet<>();

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a" + MixAll.MULTI_PATH_SPLITTER
            + "target/unit_test_store/b" + MixAll.MULTI_PATH_SPLITTER
            + "target/unit_test_store/c");

        DefaultMessageStore messageStore = new DefaultMessageStore(config,
            new BrokerStatsManager("CommitlogTest", true),
            (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {},
            new BrokerConfig(),
            new ConcurrentHashMap<>());

        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(messageStore);
        allocateMappedFileService.start();

        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, allocateMappedFileService, () -> fullStorePath);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
        assertThat(storePaths.length).isEqualTo(3);

        // the nextFilePath is "target/unit_test_store/a/00000000000000000000"
        // the first invoke will insert nextNextFilePath "target/unit_test_store/b/00000000000000001024" into requestTable
        mappedFileQueue.tryCreateMappedFile(0);

        // mark target/unit_test_store/b/ as full
        fullStorePath.add("target/unit_test_store/b");
        // the nextFilePath is still "target/unit_test_store/b/00000000000000001024"
        MappedFile mappedFile1 = mappedFileQueue.tryCreateMappedFile(1024);

        assertThat(mappedFile1).isNotNull();
        assertThat(mappedFile1.getFile().getPath()).isEqualTo("target/unit_test_store/b/00000000000000001024");

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
    }
}