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

import java.util.Random;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BitOperationUtilsTest {

    @Test
    public void testAdd() throws Exception {
        Random random = new Random();
        final int x = random.nextInt(100);
        final int y = random.nextInt(100);
        assertThat(BitOperationUtils.add(x, y)).isEqualTo(x + y);
    }

    @Test
    public void testAddOverflow() throws Exception {
        int x = Integer.MAX_VALUE;
        int y = Integer.MAX_VALUE;
        assertThat(BitOperationUtils.add(x, y)).isEqualTo(x + y)
            .isLessThan(0);
    }

    @Test
    public void testSubtract() throws Exception {
        Random random = new Random();
        final int x = random.nextInt(100);
        final int y = random.nextInt(100);
        assertThat(BitOperationUtils.subtract(x, y)).isEqualTo(x - y);
    }

    @Test
    public void testSubtractOverflow() throws Exception {
        int x = Integer.MAX_VALUE;
        int y = Integer.MIN_VALUE;
        assertThat(BitOperationUtils.subtract(x, y)).isEqualTo(x - y)
            .isLessThan(0);
    }

    @Test
    public void testMultiply() throws Exception {
        Random random = new Random();
        final int x = random.nextInt(100);
        final int y = random.nextInt(100);
        assertThat(BitOperationUtils.multiply(x, y)).isEqualTo(x * y);
    }

    @Test
    public void testMultiplyOverflow() throws Exception {
        int x = Integer.MAX_VALUE;
        int y = Integer.MAX_VALUE;
        assertThat(BitOperationUtils.multiply(x, y)).isEqualTo(x * y)
            .isEqualTo(1);
    }

    @Test
    public void testDivide() throws Exception {
        Random random = new Random();
        final int x = random.nextInt(100);
        final int y = random.nextInt(100);
        assertThat(BitOperationUtils.divide(x, y)).isEqualTo(x / y);
    }
}
