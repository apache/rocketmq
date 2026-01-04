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

package org.apache.rocketmq.tieredstore.index;

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class IndexItemTest {

    private final int topicId = 1;
    private final int queueId = 2;
    private final long offset = 3L;
    private final int size = 4;
    private final int hashCode = 5;
    private final int timeDiff = 6;
    private final int itemIndex = 7;

    @Test
    public void indexItemConstructorTest() {
        IndexItem indexItem = new IndexItem(topicId, queueId, offset, size, hashCode, timeDiff, itemIndex);

        Assert.assertEquals(topicId, indexItem.getTopicId());
        Assert.assertEquals(queueId, indexItem.getQueueId());
        Assert.assertEquals(offset, indexItem.getOffset());
        Assert.assertEquals(size, indexItem.getSize());
        Assert.assertEquals(hashCode, indexItem.getHashCode());
        Assert.assertEquals(timeDiff, indexItem.getTimeDiff());
        Assert.assertEquals(itemIndex, indexItem.getItemIndex());
    }

    @Test
    public void byteBufferConstructorTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(IndexItem.INDEX_ITEM_SIZE);
        byteBuffer.putInt(hashCode);
        byteBuffer.putInt(topicId);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(size);
        byteBuffer.putInt(timeDiff);
        byteBuffer.putInt(itemIndex);

        byte[] bytes = byteBuffer.array();
        IndexItem indexItem = new IndexItem(bytes);

        Assert.assertEquals(topicId, indexItem.getTopicId());
        Assert.assertEquals(queueId, indexItem.getQueueId());
        Assert.assertEquals(offset, indexItem.getOffset());
        Assert.assertEquals(size, indexItem.getSize());
        Assert.assertEquals(hashCode, indexItem.getHashCode());
        Assert.assertEquals(timeDiff, indexItem.getTimeDiff());
        Assert.assertEquals(itemIndex, indexItem.getItemIndex());
        Assert.assertNotNull(indexItem.toString());

        Exception exception = null;
        try {
            new IndexItem(null);
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertNotNull(exception);
    }

    @Test
    public void getByteBufferTest() {
        IndexItem indexItem = new IndexItem(topicId, queueId, offset, size, hashCode, timeDiff, itemIndex);
        ByteBuffer byteBuffer = indexItem.getByteBuffer();
        Assert.assertEquals(hashCode, byteBuffer.getInt(0));
        Assert.assertEquals(topicId, byteBuffer.getInt(4));
        Assert.assertEquals(queueId, byteBuffer.getInt(8));
        Assert.assertEquals(offset, byteBuffer.getLong(12));
        Assert.assertEquals(size, byteBuffer.getInt(20));
        Assert.assertEquals(timeDiff, byteBuffer.getInt(24));
        Assert.assertEquals(itemIndex, byteBuffer.getInt(28));
    }
}