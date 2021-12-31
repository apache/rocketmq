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
package org.apache.rocketmq.common.concurrent;

import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultOrderedExecutorServiceTest {

    static OrderedExecutorService executor = new DefaultOrderedExecutorService("test", 7);

    @After
    public void shutdown() {
        executor.shutdown();
    }

    @Test
    public void testRoundRibbon() throws Exception {
        int last = -1;
        for (int i = 0; i < 1000; i++) {
            int code = executor.computeCode();
            assertThat(last).isLessThan(code);
            last = code;
        }
    }

    @Test
    public void testHash() throws Exception {
        int code = executor.computeCode("topic");
        for (int i = 0; i < 1000; i++) {
            assertThat(executor.computeCode("topic")).isEqualTo(code);
        }
        code = executor.computeCode("topic", 1);
        for (int i = 0; i < 1000; i++) {
            assertThat(executor.computeCode("topic", 1)).isEqualTo(code);
        }
    }
}
