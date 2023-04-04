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

package org.apache.rocketmq.tieredstore.provider.s3;

import org.junit.Assert;
import org.junit.Test;

public class S3FileSegmentMetadataTest {

    @Test
    public void testBasicOperation() {
        S3FileSegmentMetadata metadata = new S3FileSegmentMetadata();
        // valid chunk adding
        Assert.assertTrue(metadata.addChunk(new ChunkMetadata("test", 0, 10)));
        Assert.assertTrue(metadata.addChunk(new ChunkMetadata("test", 10, 10)));
        Assert.assertEquals(0, metadata.getStartPosition());
        Assert.assertEquals(19, metadata.getEndPosition());
        Assert.assertEquals(20, metadata.getSize());
        Assert.assertEquals(2, metadata.getChunkCount());
        Assert.assertFalse(metadata.isSealed());

        // invalid chunk adding
        Assert.assertFalse(metadata.addChunk(new ChunkMetadata("test", 0, 10)));

        // seal
        metadata.setSegment(new ChunkMetadata("test", 0, 10));
        Assert.assertTrue(metadata.isSealed());
        Assert.assertEquals(0, metadata.getStartPosition());
        Assert.assertEquals(9, metadata.getEndPosition());
        Assert.assertEquals(10, metadata.getSize());
        Assert.assertEquals(2, metadata.getChunkCount());

        // remove all chunks
        metadata.removeAllChunks();
        Assert.assertEquals(0, metadata.getChunkCount());

    }

}
