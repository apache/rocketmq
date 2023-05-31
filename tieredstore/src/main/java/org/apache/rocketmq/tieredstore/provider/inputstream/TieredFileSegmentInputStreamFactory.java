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

package org.apache.rocketmq.tieredstore.provider.inputstream;

import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

import java.nio.ByteBuffer;
import java.util.List;

public class TieredFileSegmentInputStreamFactory {

    public static TieredFileSegmentInputStream build(TieredFileSegment.FileSegmentType fileType,
                                                     long startOffset,
                                                     List<ByteBuffer> uploadBufferList,
                                                     ByteBuffer codaBuffer,
                                                     int contentLength) {
        if (fileType == TieredFileSegment.FileSegmentType.COMMIT_LOG) {
            return new TieredCommitLogInputStream(fileType, startOffset, uploadBufferList, codaBuffer, contentLength);
        } else if (fileType == TieredFileSegment.FileSegmentType.CONSUME_QUEUE) {
            return new TieredFileSegmentInputStream(fileType, uploadBufferList, contentLength);
        } else if (fileType == TieredFileSegment.FileSegmentType.INDEX) {
            if (uploadBufferList.size() != 1) {
                throw new IllegalArgumentException("uploadBufferList size in INDEX type input stream must be 1");
            }
            return new TieredFileSegmentInputStream(fileType, uploadBufferList, contentLength);
        } else {
            throw new IllegalArgumentException("fileType is not supported");
        }
    }
}
