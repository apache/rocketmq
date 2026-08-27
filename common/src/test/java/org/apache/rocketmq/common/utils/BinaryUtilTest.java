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
package org.apache.rocketmq.common.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BinaryUtilTest {

    @Test
    public void testGenerateMd5MatchesFreshDigestAndIsStableAcrossCalls() throws Exception {
        byte[] payload = "rocketmq-md5-test".getBytes(StandardCharsets.UTF_8);
        String expected = Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(payload), false);

        String first = BinaryUtil.generateMd5(payload);
        // interleave another digest to verify the reused per-thread instance is reset between calls
        BinaryUtil.generateMd5("another-payload".getBytes(StandardCharsets.UTF_8));
        String second = BinaryUtil.generateMd5(payload);

        assertEquals(expected, first);
        assertEquals(expected, second);
    }

    @Test
    public void testGenerateMd5FromString() throws Exception {
        String body = "string-body-\u4e2d\u6587";
        String expected = Hex.encodeHexString(
            MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8)), false);
        assertEquals(expected, BinaryUtil.generateMd5(body));
    }
}
