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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class TestUtil {

    private TestUtil() {
    }

    public static Long parseStringToLong(String s, Long defval) {
        Long val = defval;
        try {
            val = Long.parseLong(s);
        } catch (NumberFormatException e) {
            val = defval;
        }
        return val;
    }

    public static Integer parseStringToInteger(String s, Integer defval) {
        Integer val = defval;
        try {
            val = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            val = defval;
        }
        return val;
    }

    public static String addQuoteToParamater(String param) {
        StringBuilder sb = new StringBuilder("'");
        sb.append(param).append("'");
        return sb.toString();
    }

    public static void waitForMonment(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitForSeconds(long time) {
        try {
            TimeUnit.SECONDS.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitForMinutes(long time) {
        try {
            TimeUnit.MINUTES.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitForInputQuit() {
        waitForInput("quit");
    }

    public static void waitForInput(String keyWord) {
        waitForInput(keyWord,
            String.format("The thread will wait until you input stop command[%s]:", keyWord));
    }

    public static void waitForInput(String keyWord, String info) {
        try {
            byte[] b = new byte[1024];
            int n = System.in.read(b);
            String s = new String(b, 0, n - 1, StandardCharsets.UTF_8).replace("\r", "").replace("\n", "");
            while (!s.equals(keyWord)) {
                n = System.in.read(b);
                s = new String(b, 0, n - 1, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

}
