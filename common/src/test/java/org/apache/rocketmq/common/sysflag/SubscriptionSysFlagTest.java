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
package org.apache.rocketmq.common.sysflag;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionSysFlagTest {

    private static final int UNIT = 0x1;

    @Test
    public void testBuildSysFlag() {
        assertThat(SubscriptionSysFlag.buildSysFlag(false)).isEqualTo(0);
        assertThat(SubscriptionSysFlag.buildSysFlag(true)).isEqualTo(UNIT);
    }

    @Test
    public void testUnitFlag() {
        int flag = SubscriptionSysFlag.setUnitFlag(0);
        assertThat(SubscriptionSysFlag.hasUnitFlag(flag)).isTrue();
        assertThat(SubscriptionSysFlag.setUnitFlag(flag)).isEqualTo(flag);

        flag = SubscriptionSysFlag.clearUnitFlag(flag);
        assertThat(SubscriptionSysFlag.hasUnitFlag(flag)).isFalse();
        assertThat(flag).isEqualTo(0);
        assertThat(SubscriptionSysFlag.clearUnitFlag(flag)).isEqualTo(flag);
    }

    @Test
    public void testOtherBitsArePreserved() {
        int otherBits = 0xFF0;

        assertThat(SubscriptionSysFlag.setUnitFlag(otherBits)).isEqualTo(otherBits | UNIT);
        assertThat(SubscriptionSysFlag.clearUnitFlag(otherBits | UNIT)).isEqualTo(otherBits);
        assertThat(SubscriptionSysFlag.hasUnitFlag(otherBits)).isFalse();
    }
}
