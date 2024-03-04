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
package org.apache.rocketmq.tieredstore.provider;

import java.io.IOException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

public class MemoryFileSegmentTest {

    @Test
    public void memoryTest() throws IOException {
        MemoryFileSegment fileSegment = new MemoryFileSegment(
            new MessageStoreConfig(), FileSegmentType.COMMIT_LOG,
            MessageStoreUtil.toFilePath(new MessageQueue()), 0L);
        Assert.assertFalse(fileSegment.exists());
        fileSegment.createFile();
        MemoryFileSegment fileSpySegment = Mockito.spy(fileSegment);
        FileSegmentInputStream inputStream = Mockito.mock(FileSegmentInputStream.class);
        Mockito.when(inputStream.read(any())).thenThrow(new RuntimeException());
        Assert.assertFalse(fileSpySegment.commit0(inputStream, 0L, 0, false).join());
        fileSegment.destroyFile();
    }
}