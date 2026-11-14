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
package org.apache.rocketmq.store.rocksdb;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Stream;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MessageRocksDBStorageLifecycleTest {
    private MessageRocksDBStorage storage;
    private Path storePath;

    @Before
    public void setUp() throws Exception {
        Path testRoot = Paths.get("target", "rocketmq-lifecycle-test");
        Files.createDirectories(testRoot);
        storePath = Files.createTempDirectory(testRoot, "message-rocksdb-");
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(storePath.toString());
        storage = new MessageRocksDBStorage(config);
    }

    @After
    public void tearDown() throws Exception {
        if (storage != null) {
            storage.shutdown();
        }
        if (storePath != null) {
            try (Stream<Path> paths = Files.walk(storePath)) {
                paths.sorted((first, second) -> second.compareTo(first))
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }
    }

    @Test
    public void shouldStopSchedulerOnRepeatedShutdownAndCreateSingleTaskOnRestart() throws Exception {
        ScheduledThreadPoolExecutor previousScheduler = getScheduler();
        Assert.assertEquals(1, previousScheduler.getQueue().size());

        for (int i = 0; i < 2; i++) {
            Assert.assertTrue(storage.shutdown());
            Assert.assertTrue(previousScheduler.isShutdown());
            Assert.assertEquals(0, previousScheduler.getQueue().size());

            Assert.assertTrue(storage.start());
            ScheduledThreadPoolExecutor currentScheduler = getScheduler();
            Assert.assertNotSame(previousScheduler, currentScheduler);
            Assert.assertFalse(currentScheduler.isShutdown());
            Assert.assertEquals(1, currentScheduler.getQueue().size());
            previousScheduler = currentScheduler;
        }
    }

    private ScheduledThreadPoolExecutor getScheduler() throws Exception {
        Field schedulerField = MessageRocksDBStorage.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        return (ScheduledThreadPoolExecutor) schedulerField.get(storage);
    }
}
