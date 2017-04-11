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

package org.apache.rocketmq.filter.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

/**
 * Note that all hash
 * algorithms return 64-bits of hash, but only the lower 32-bits are
 * significant. This allows a positive 32-bit number to be returned for all
 * cases.
 * <br>Don't change the order of algorithms, add new algorithm to last if you want.
 */
public enum HashAlgorithm {

    /**
     * Native hash (String.hashCode()).
     */
    NATIVE_HASH,
    /**
     * CRC32_HASH as used by the perl API. This will be more consistent both
     * across multiple API users as well as java versions, but is mostly likely
     * significantly slower.
     */
    CRC32_HASH,
    /**
     * FNV hashes are designed to be fast while maintaining a low collision
     * rate. The FNV speed allows one to quickly hash lots of data while
     * maintaining a reasonable collision rate.
     */
    FNV1_64_HASH,
    /**
     * Variation of FNV.
     */
    FNV1A_64_HASH,
    /**
     * 32-bit FNV1.
     */
    FNV1_32_HASH,
    /**
     * 32-bit FNV1a.
     */
    FNV1A_32_HASH,
    /**
     * MD5-based hash algorithm used by ketama.
     */
    KETAMA_HASH,

    /**
     * From mysql source
     */
    MYSQL_HASH,

    ELF_HASH,

    RS_HASH,

    /**
     * From lua source,it is used for long key
     */
    LUA_HASH,

    ELECTION_HASH,
    /**
     * The Jenkins One-at-a-time hash ,please see
     * http://www.burtleburtle.net/bob/hash/doobs.html
     */
    ONE_AT_A_TIME;

    private static final long FNV_64_INIT = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    private static final long FNV_32_INIT = 2166136261L;
    private static final long FNV_32_PRIME = 16777619;


    /**
     * Compute the hash for the given key.
     *
     * @return a positive integer hash
     */
    public long hash(final String k) {
        long rv = 0;
        switch (this) {
            case NATIVE_HASH:
                rv = k.hashCode();
                break;
            case CRC32_HASH:
                // return (crc32(shift) >> 16) & 0x7fff;
                final CRC32 crc32 = new CRC32();
                crc32.update(getBytes(k));
                rv = crc32.getValue() >> 16 & 0x7fff;
                break;
            case FNV1_64_HASH: {
                // Thanks to pierre@demartines.com for the pointer
                rv = FNV_64_INIT;
                final int len = k.length();
                for (int i = 0; i < len; i++) {
                    rv *= FNV_64_PRIME;
                    rv ^= k.charAt(i);
                }
            }
            break;
            case FNV1A_64_HASH: {
                rv = FNV_64_INIT;
                final int len = k.length();
                for (int i = 0; i < len; i++) {
                    rv ^= k.charAt(i);
                    rv *= FNV_64_PRIME;
                }
            }
            break;
            case FNV1_32_HASH: {
                rv = FNV_32_INIT;
                final int len = k.length();
                for (int i = 0; i < len; i++) {
                    rv *= FNV_32_PRIME;
                    rv ^= k.charAt(i);
                }
            }
            break;
            case FNV1A_32_HASH: {
                rv = FNV_32_INIT;
                final int len = k.length();
                for (int i = 0; i < len; i++) {
                    rv ^= k.charAt(i);
                    rv *= FNV_32_PRIME;
                }
            }
            break;
            case ELECTION_HASH:
            case KETAMA_HASH:
                final byte[] bKey = computeMd5(k);
                rv =
                    (long) (bKey[3] & 0xFF) << 24 | (long) (bKey[2] & 0xFF) << 16
                        | (long) (bKey[1] & 0xFF) << 8 | bKey[0] & 0xFF;
                break;

            case MYSQL_HASH:
                int nr2 = 4;
                for (int i = 0; i < k.length(); i++) {
                    rv ^= ((rv & 63) + nr2) * k.charAt(i) + (rv << 8);
                    nr2 += 3;
                }
                break;
            case ELF_HASH:
                long x = 0;
                for (int i = 0; i < k.length(); i++) {
                    rv = (rv << 4) + k.charAt(i);
                    if ((x = rv & 0xF0000000L) != 0) {
                        rv ^= x >> 24;
                        rv &= ~x;
                    }
                }
                rv = rv & 0x7FFFFFFF;
                break;
            case RS_HASH:
                final long b = 378551;
                long a = 63689;
                for (int i = 0; i < k.length(); i++) {
                    rv = rv * a + k.charAt(i);
                    a *= b;
                }
                rv = rv & 0x7FFFFFFF;
                break;
            case LUA_HASH:
                final int step = (k.length() >> 5) + 1;
                rv = k.length();
                for (int len = k.length(); len >= step; len -= step) {
                    rv = rv ^ (rv << 5) + (rv >> 2) + k.charAt(len - 1);
                }
                break;
            case ONE_AT_A_TIME:
                try {
                    int hash = 0;
                    for (final byte bt : k.getBytes("utf-8")) {
                        hash += bt & 0xFF;
                        hash += hash << 10;
                        hash ^= hash >>> 6;
                    }
                    hash += hash << 3;
                    hash ^= hash >>> 11;
                    hash += hash << 15;
                    return hash;
                } catch (final UnsupportedEncodingException e) {
                    throw new IllegalStateException("Hash function error", e);
                }
            default:
                assert false;
        }

        // Truncate to 32-bits
        return rv & 0xffffffffL;
    }

    /**
     * Get the md5 of the given key.
     */
    public static byte[] computeMd5(final String k) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        md5.update(getBytes(k));
        return md5.digest();
    }

    public static final byte[] getBytes(final String k) {
        if (k == null || k.length() == 0) {
            return null;
        }
        try {
            return k.getBytes("UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
