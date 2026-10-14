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

public class TopicSysFlagTest {

    private static final int UNIT = 0x1;
    private static final int UNIT_SUB = 0x2;

    @Test
    public void testBuildSysFlag() {
        assertThat(TopicSysFlag.buildSysFlag(false, false)).isEqualTo(0);
        assertThat(TopicSysFlag.buildSysFlag(true, false)).isEqualTo(UNIT);
        assertThat(TopicSysFlag.buildSysFlag(false, true)).isEqualTo(UNIT_SUB);
        assertThat(TopicSysFlag.buildSysFlag(true, true)).isEqualTo(UNIT | UNIT_SUB);
    }

    @Test
    public void testUnitFlag() {
        int flag = TopicSysFlag.setUnitFlag(0);
        assertThat(TopicSysFlag.hasUnitFlag(flag)).isTrue();
        assertThat(TopicSysFlag.hasUnitSubFlag(flag)).isFalse();
        assertThat(TopicSysFlag.setUnitFlag(flag)).isEqualTo(flag);

        flag = TopicSysFlag.clearUnitFlag(flag);
        assertThat(TopicSysFlag.hasUnitFlag(flag)).isFalse();
        assertThat(flag).isEqualTo(0);
        assertThat(TopicSysFlag.clearUnitFlag(flag)).isEqualTo(flag);
    }

    @Test
    public void testUnitSubFlag() {
        int flag = TopicSysFlag.setUnitSubFlag(0);
        assertThat(TopicSysFlag.hasUnitSubFlag(flag)).isTrue();
        assertThat(TopicSysFlag.hasUnitFlag(flag)).isFalse();
        assertThat(TopicSysFlag.setUnitSubFlag(flag)).isEqualTo(flag);

        flag = TopicSysFlag.clearUnitSubFlag(flag);
        assertThat(TopicSysFlag.hasUnitSubFlag(flag)).isFalse();
        assertThat(flag).isEqualTo(0);
        assertThat(TopicSysFlag.clearUnitSubFlag(flag)).isEqualTo(flag);
    }

    @Test
    public void testFlagsAreIndependent() {
        int flag = TopicSysFlag.buildSysFlag(true, true);

        int withoutUnit = TopicSysFlag.clearUnitFlag(flag);
        assertThat(TopicSysFlag.hasUnitFlag(withoutUnit)).isFalse();
        assertThat(TopicSysFlag.hasUnitSubFlag(withoutUnit)).isTrue();

        int withoutUnitSub = TopicSysFlag.clearUnitSubFlag(flag);
        assertThat(TopicSysFlag.hasUnitFlag(withoutUnitSub)).isTrue();
        assertThat(TopicSysFlag.hasUnitSubFlag(withoutUnitSub)).isFalse();
    }

    @Test
    public void testOtherBitsArePreserved() {
        int otherBits = 0xFF0;

        assertThat(TopicSysFlag.setUnitFlag(otherBits)).isEqualTo(otherBits | UNIT);
        assertThat(TopicSysFlag.clearUnitFlag(otherBits | UNIT)).isEqualTo(otherBits);
        assertThat(TopicSysFlag.setUnitSubFlag(otherBits)).isEqualTo(otherBits | UNIT_SUB);
        assertThat(TopicSysFlag.clearUnitSubFlag(otherBits | UNIT_SUB)).isEqualTo(otherBits);
    }
}
