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

package org.apache.rocketmq.client.consumer.store;


import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ControllableOffsetTest {

    private ControllableOffset controllableOffset;

    @Before
    public void setUp() {
        controllableOffset = new ControllableOffset(0);
    }

    @Test
    public void testUpdateAndFreeze_ShouldFreezeOffsetAtTargetValue() {
        controllableOffset.updateAndFreeze(100);
        assertEquals(100, controllableOffset.getOffset());
        controllableOffset.update(200);
        assertEquals(100, controllableOffset.getOffset());
    }

    @Test
    public void testUpdate_ShouldUpdateOffsetWhenNotFrozen() {
        controllableOffset.update(200);
        assertEquals(200, controllableOffset.getOffset());
    }

    @Test
    public void testUpdate_ShouldNotUpdateOffsetWhenFrozen() {
        controllableOffset.updateAndFreeze(100);
        controllableOffset.update(200);
        assertEquals(100, controllableOffset.getOffset());
    }

    @Test
    public void testUpdate_ShouldNotDecreaseOffsetWhenIncreaseOnly() {
        controllableOffset.update(200);
        controllableOffset.update(100, true);
        assertEquals(200, controllableOffset.getOffset());
    }

    @Test
    public void testUpdate_ShouldUpdateOffsetToGreaterValueWhenIncreaseOnly() {
        controllableOffset.update(100);
        controllableOffset.update(200, true);
        assertEquals(200, controllableOffset.getOffset());
    }

}
