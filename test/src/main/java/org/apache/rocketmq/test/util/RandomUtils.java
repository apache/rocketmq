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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util;

import java.util.Random;
import java.util.UUID;

public class RandomUtils {
    private static final int UNICODE_START = '\u4E00';
    private static final int UNICODE_END = '\u9FA0';
    private static Random rd = new Random();

    private RandomUtils() {

    }

    public static String getStringByUUID() {
        return UUID.randomUUID().toString();
    }

    public static String getCheseWord(int len) {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < len; ++i) {
            char str = getCheseChar();
            res.append(str);
        }

        return res.toString();
    }

    public static String getStringWithNumber(int n) {
        int arg[] = new int[] {'0', '9' + 1};
        return getString(n, arg);
    }

    public static String getStringWithCharacter(int n) {
        int arg[] = new int[] {'a', 'z' + 1, 'A', 'Z' + 1};
        return getString(n, arg);
    }

    private static String getString(int n, int arg[]) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < n; i++) {
            res.append(getChar(arg));
        }
        return res.toString();
    }

    private static char getChar(int arg[]) {
        int size = arg.length;
        int c = rd.nextInt(size / 2);
        c = c * 2;
        return (char) (getIntegerBetween(arg[c], arg[c + 1]));
    }

    public static int getIntegerBetween(int n, int m) {
        if (m == n) {
            return n;
        }
        int res = getIntegerMoreThanZero();
        return n + res % (m - n);
    }

    public static int getIntegerMoreThanZero() {
        int res = rd.nextInt();
        while (res <= 0) {
            res = rd.nextInt();
        }
        return res;
    }

    private static char getCheseChar() {
        return (char) (UNICODE_START + rd.nextInt(UNICODE_END - UNICODE_START));
    }
}
