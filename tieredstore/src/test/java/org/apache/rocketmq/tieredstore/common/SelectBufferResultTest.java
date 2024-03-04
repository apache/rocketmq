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
package org.apache.rocketmq.tieredstore.common;

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class SelectBufferResultTest {

    @Test
    public void selectBufferResultTest() {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        long startOffset = 5L;
        int size = 10;
        long tagCode = 1L;

        SelectBufferResult result = new SelectBufferResult(buffer, startOffset, size, tagCode);
        Assert.assertEquals(buffer, result.getByteBuffer());
        Assert.assertEquals(startOffset, result.getStartOffset());
        Assert.assertEquals(size, result.getSize());
        Assert.assertEquals(tagCode, result.getTagCode());
    }
}