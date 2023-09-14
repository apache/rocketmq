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

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

public class BinaryUtil {
    public static byte[] calculateMd5(byte[] binaryData) {
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found.");
        }
        messageDigest.update(binaryData);
        return messageDigest.digest();
    }

    public static String generateMd5(String bodyStr) {
        byte[] bytes = calculateMd5(bodyStr.getBytes(Charset.forName("UTF-8")));
        return Hex.encodeHexString(bytes, false);
    }

    public static String generateMd5(byte[] content) {
        byte[] bytes = calculateMd5(content);
        return Hex.encodeHexString(bytes, false);
    }

    /**
     * Returns true if subject contains only bytes that are spec-compliant ASCII characters.
     * @param subject
     * @return
     */
    public static boolean isAscii(byte[] subject) {
        if (subject == null) {
            return false;
        }
        for (byte b : subject) {
            if ((b & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }
}