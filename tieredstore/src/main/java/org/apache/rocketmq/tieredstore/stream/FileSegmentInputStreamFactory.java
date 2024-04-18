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

package org.apache.rocketmq.tieredstore.stream;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;

public class FileSegmentInputStreamFactory {

    public static FileSegmentInputStream build(
        FileSegmentType fileType, long offset, List<ByteBuffer> bufferList, ByteBuffer byteBuffer, int length) {

        if (bufferList == null) {
            throw new IllegalArgumentException("bufferList is null");
        }

        switch (fileType) {
            case COMMIT_LOG:
                return new CommitLogInputStream(fileType, offset, bufferList, byteBuffer, length);
            case CONSUME_QUEUE:
                return new FileSegmentInputStream(fileType, bufferList, length);
            case INDEX:
            case INDEX_COMPACTED:
                if (bufferList.size() != 1) {
                    throw new IllegalArgumentException("buffer block size must be 1 when file type is IndexFile");
                }
                return new FileSegmentInputStream(fileType, bufferList, length);
            default:
                throw new IllegalArgumentException("file type not supported");
        }
    }
}
