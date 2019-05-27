/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.mqtt.util.orderedexecutor;

import java.util.concurrent.TimeUnit;

/**
 * Provides misc math functions that don't come standard.
 */
public class MathUtils {

    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

    public static int signSafeMod(long dividend, int divisor) {
        int mod = (int) (dividend % divisor);

        if (mod < 0) {
            mod += divisor;
        }

        return mod;
    }

    public static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Current time from some arbitrary time base in the past, counting in
     * nanoseconds, and not affected by settimeofday or similar system clock
     * changes. This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     *
     * <p>NOTE: only use it for measuring.
     * http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
     *
     * @return current time in nanoseconds.
     */
    public static long nowInNano() {
        return System.nanoTime();
    }

    /**
     * Milliseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time.
     *
     * @param startNanoTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedMSec(long startNanoTime) {
       return (System.nanoTime() - startNanoTime) / NANOSECONDS_PER_MILLISECOND;
    }

    /**
     * Microseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time.
     *
     * @param startNanoTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedMicroSec(long startNanoTime) {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanoTime);
    }

    /**
     * Nanoseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time.
     *
     * @param startNanoTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedNanos(long startNanoTime) {
       return System.nanoTime() - startNanoTime;
    }
}
