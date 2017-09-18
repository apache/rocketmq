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

package org.apache.rocketmq.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MQVersionTest {

    @Test
    public void testGetVersionDesc() throws Exception {
        String desc = "V3_0_0_SNAPSHOT";
        assertThat(MQVersion.getVersionDesc(0)).isEqualTo(desc);
    }

    @Test
    public void testGetVersionDesc_higherVersion() throws Exception {
        String desc = "HIGHER_VERSION";
        assertThat(MQVersion.getVersionDesc(Integer.MAX_VALUE)).isEqualTo(desc);
    }

    @Test
    public void testValue2Version() throws Exception {
        assertThat(MQVersion.value2Version(0)).isEqualTo(MQVersion.Version.V3_0_0_SNAPSHOT);
    }

    @Test
    public void testValue2Version_HigherVersion() throws Exception {
        assertThat(MQVersion.value2Version(Integer.MAX_VALUE)).isEqualTo(MQVersion.Version.HIGHER_VERSION);
    }
}