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

public class BitOperationUtils {

    /**
     * just like "x+y", but better performance
     * Note that this method has problems when the result value overflows
     */
    public static int add(int x, int y) {
        while (y != 0) {
            int sum = x ^ y;
            y = (x & y) << 1;
            x = sum;
        }
        return x;
    }

    /**
     * just like "x-y", but better performance
     * Note that this method has problems when the result value overflows
     */
    public static int subtract(int x, int y) {
        return add(x, getNegative(y));
    }

    /**
     * just like "x*y", but better performance
     * Note that this method has problems when the result value overflows
     */
    public static int multiply(int x, int y) {
        boolean flag = getSign(x) == getSign(y);
        int ans = 0;
        if (x == Integer.MIN_VALUE && y == Integer.MIN_VALUE) {
            return 0;
        }
        if (y == Integer.MIN_VALUE) {
            return 0;
        }
        if (x == Integer.MIN_VALUE) {
            if (y == -1) {
                return Integer.MAX_VALUE;
            }
            ans = 1;
            x = add(x, getPositive(y));
        }
        x = getPositive(x);
        y = getPositive(y);
        while (y != 0) {
            if ((y & 1) != 0) {
                ans = add(ans, x);
            }
            x <<= 1;
            y >>= 1;
        }
        return flag ? ans : getNegative(ans);
    }

    /**
     * just like "x/y", but better performance
     * Note that this method has problems when the result value overflows
     */
    public static int divide(int a, int b) {
        boolean flag = getSign(a) == getSign(b);
        a = getPositive(a);
        b = getPositive(b);
        int ans = 0;
        for (int i = 31; i >= 0; i--) {
            if (b <= (a >> i)) {
                ans = add(ans, 1 << i);
                a = subtract(a, b << i);
            }
        }
        return flag ? ans : getNegative(ans);
    }

    public static int getSign(int i) {
        return i >> 31;
    }

    public static int getNegative(int i) {
        return add(~i, 1);
    }

    public static int getPositive(int i) {
        if ((i >> 31) != 0) {
            return getNegative(i);
        } else {
            return i;
        }
    }
}
