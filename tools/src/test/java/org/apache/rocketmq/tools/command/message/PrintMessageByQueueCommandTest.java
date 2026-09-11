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
package org.apache.rocketmq.tools.command.message;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PrintMessageByQueueCommandTest {

    @Test
    public void testTagCountComparisonWithoutOverflow() {
        PrintMessageByQueueCommand.TagCountBean most =
            new PrintMessageByQueueCommand.TagCountBean("most", new AtomicLong(Long.MAX_VALUE));
        PrintMessageByQueueCommand.TagCountBean least =
            new PrintMessageByQueueCommand.TagCountBean("least", new AtomicLong(0));

        assertTrue(most.compareTo(least) < 0);
        assertTrue(least.compareTo(most) > 0);
    }
}
