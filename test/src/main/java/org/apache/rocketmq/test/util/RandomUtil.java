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

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public final class RandomUtil {

    private static final int UNICODE_START = '\u4E00';
    private static final int UNICODE_END = '\u9FA0';
    private static Random rd = new Random();

    private RandomUtil() {

    }

    public static long getLong() {
        return rd.nextLong();
    }

    public static long getLongMoreThanZero() {
        long res = rd.nextLong();
        while (res <= 0) {
            res = rd.nextLong();
        }
        return res;
    }

    public static long getLongLessThan(long n) {
        long res = rd.nextLong();
        return res % n;
    }

    public static long getLongMoreThanZeroLessThan(long n) {
        long res = getLongLessThan(n);
        while (res <= 0) {
            res = getLongLessThan(n);
        }
        return res;
    }

    public static long getLongBetween(long n, long m) {
        if (m <= n) {
            return n;
        }
        long res = getLongMoreThanZero();
        return n + res % (m - n);
    }

    public static int getInteger() {
        return rd.nextInt();
    }

    public static int getIntegerMoreThanZero() {
        int res = rd.nextInt();
        while (res <= 0) {
            res = rd.nextInt();
        }
        return res;
    }

    public static int getIntegerLessThan(int n) {
        int res = rd.nextInt();
        return res % n;
    }

    public static int getIntegerMoreThanZeroLessThan(int n) {
        int res = rd.nextInt(n);
        while (res == 0) {
            res = rd.nextInt(n);
        }
        return res;
    }

    public static int getIntegerBetween(int n, int m)// m��ֵ����Ϊ���أ�
    {
        if (m == n) {
            return n;
        }
        int res = getIntegerMoreThanZero();
        return n + res % (m - n);
    }

    private static char getChar(int[] arg) {
        int size = arg.length;
        int c = rd.nextInt(size / 2);
        c = c * 2;
        return (char) (getIntegerBetween(arg[c], arg[c + 1]));
    }

    private static String getString(int n, int[] arg) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < n; i++) {
            res.append(getChar(arg));
        }
        return res.toString();
    }

    public static String getStringWithCharacter(int n) {
        int[] arg = new int[] {'a', 'z' + 1, 'A', 'Z' + 1};
        return getString(n, arg);
    }

    public static String getStringWithNumber(int n) {
        int[] arg = new int[] {'0', '9' + 1};
        return getString(n, arg);
    }

    public static String getStringWithNumAndCha(int n) {
        int[] arg = new int[] {'a', 'z' + 1, 'A', 'Z' + 1, '0', '9' + 1};
        return getString(n, arg);
    }

    public static String getStringShortenThan(int n) {
        int len = getIntegerMoreThanZeroLessThan(n);
        return getStringWithCharacter(len);
    }

    public static String getStringWithNumAndChaShortenThan(int n) {
        int len = getIntegerMoreThanZeroLessThan(n);
        return getStringWithNumAndCha(len);
    }

    public static String getStringBetween(int n, int m) {
        int len = getIntegerBetween(n, m);
        return getStringWithCharacter(len);
    }

    public static String getStringWithNumAndChaBetween(int n, int m) {
        int len = getIntegerBetween(n, m);
        return getStringWithNumAndCha(len);
    }

    public static String getStringWithPrefix(int n, String prefix) {
        int len = prefix.length();
        if (n <= len)
            return prefix;
        else {
            len = n - len;
            StringBuilder res = new StringBuilder(prefix);
            res.append(getStringWithCharacter(len));
            return res.toString();
        }
    }

    public static String getStringWithSuffix(int n, String suffix) {

        int len = suffix.length();
        if (n <= len)
            return suffix;
        else {
            len = n - len;
            StringBuilder res = new StringBuilder();
            res.append(getStringWithCharacter(len));
            res.append(suffix);
            return res.toString();
        }
    }

    public static String getStringWithBoth(int n, String prefix, String suffix) {
        int len = prefix.length() + suffix.length();
        StringBuilder res = new StringBuilder(prefix);
        if (n <= len)
            return res.append(suffix).toString();
        else {
            len = n - len;
            res.append(getStringWithCharacter(len));
            res.append(suffix);
            return res.toString();
        }
    }

    public static String getCheseWordWithPrifix(int n, String prefix) {
        int len = prefix.length();
        if (n <= len)
            return prefix;
        else {
            len = n - len;
            StringBuilder res = new StringBuilder(prefix);
            res.append(getCheseWord(len));
            return res.toString();
        }
    }

    public static String getCheseWordWithSuffix(int n, String suffix) {

        int len = suffix.length();
        if (n <= len)
            return suffix;
        else {
            len = n - len;
            StringBuilder res = new StringBuilder();
            res.append(getCheseWord(len));
            res.append(suffix);
            return res.toString();
        }
    }

    public static String getCheseWordWithBoth(int n, String prefix, String suffix) {
        int len = prefix.length() + suffix.length();
        StringBuilder res = new StringBuilder(prefix);
        if (n <= len)
            return res.append(suffix).toString();
        else {
            len = n - len;
            res.append(getCheseWord(len));
            res.append(suffix);
            return res.toString();
        }
    }

    public static String getCheseWord(int len) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < len; i++) {
            char str = getCheseChar();
            res.append(str);
        }
        return res.toString();
    }

    private static char getCheseChar() {
        return (char) (UNICODE_START + rd.nextInt(UNICODE_END - UNICODE_START));
    }

    public static boolean getBoolean() {
        return getIntegerMoreThanZeroLessThan(3) == 1;
    }

    public static String getStringByUUID() {
        return UUID.randomUUID().toString();
    }

    public static int[] getRandomArray(int min, int max, int n) {
        int len = max - min + 1;

        if (max < min || n > len) {
            return null;
        }

        int[] source = new int[len];
        for (int i = min; i < min + len; i++) {
            source[i - min] = i;
        }

        int[] result = new int[n];
        Random rd = new Random();
        int index = 0;
        for (int i = 0; i < result.length; i++) {
            index = rd.nextInt(len--);
            result[i] = source[index];
            source[index] = source[len];
        }
        return result;
    }

    public static Collection<Integer> getRandomCollection(int min, int max, int n) {
        Set<Integer> res = new HashSet<Integer>();
        int mx = max;
        int mn = min;
        if (n == (max + 1 - min)) {
            for (int i = 1; i <= n; i++) {
                res.add(i);
            }
            return res;
        }
        for (int i = 0; i < n; i++) {
            int v = getIntegerBetween(mn, mx);
            if (v == mx) {
                mx--;
            }
            if (v == mn) {
                mn++;
            }
            while (res.contains(v)) {
                v = getIntegerBetween(mn, mx);
                if (v == mx) {
                    mx = v;
                }
                if (v == mn) {
                    mn = v;
                }
            }
            res.add(v);
        }
        return res;
    }
}
