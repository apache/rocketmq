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
package org.apache.rocketmq.tieredstore.provider.posix;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PosixFileSegmentTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();
    private TieredMessageStoreConfig storeConfig;
    private MessageQueue mq;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setTieredStoreFilePath(storePath);
        mq = new MessageQueue("OSSFileSegmentTest", "broker", 0);
        TieredStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyCompositeFlatFileManager();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void testCommitAndRead() throws IOException {
        PosixFileSegment fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.CONSUME_QUEUE, TieredStoreUtil.toPath(mq), 0);
        byte[] source = new byte[4096];
        new Random().nextBytes(source);
        ByteBuffer buffer = ByteBuffer.wrap(source);
        fileSegment.append(buffer, 0);
        fileSegment.commit();

        File file = new File(fileSegment.getPath());
        Assert.assertTrue(file.exists());
        byte[] result = new byte[4096];
        ByteStreams.read(Files.asByteSource(file).openStream(), result, 0, 4096);
        Assert.assertArrayEquals(source, result);

        ByteBuffer read = fileSegment.read(0, 4096);
        Assert.assertArrayEquals(source, read.array());
    }
}
