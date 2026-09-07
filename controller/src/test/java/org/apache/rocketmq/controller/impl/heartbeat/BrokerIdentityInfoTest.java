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
package org.apache.rocketmq.controller.impl.heartbeat;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerIdentityInfoTest {

    @Test
    public void testEqualsCompleteIdentity() {
        BrokerIdentityInfo identity = new BrokerIdentityInfo("cluster", "broker", 1L);
        BrokerIdentityInfo sameIdentity = new BrokerIdentityInfo("cluster", "broker", 1L);

        assertThat(identity).isEqualTo(sameIdentity);
        assertThat(identity.hashCode()).isEqualTo(sameIdentity.hashCode());
    }

    @Test
    public void testEqualsPartialIdentity() {
        assertThat(new BrokerIdentityInfo(null, "broker", null))
            .isEqualTo(new BrokerIdentityInfo(null, "broker", null));
        assertThat(new BrokerIdentityInfo("cluster", null, null))
            .isEqualTo(new BrokerIdentityInfo("cluster", null, null));
        assertThat(new BrokerIdentityInfo("cluster", "broker", null))
            .isEqualTo(new BrokerIdentityInfo("cluster", "broker", null));
    }

    @Test
    public void testEqualsDifferentIdentity() {
        BrokerIdentityInfo identity = new BrokerIdentityInfo("cluster", "broker", 1L);

        assertThat(identity).isNotEqualTo(new BrokerIdentityInfo("other-cluster", "broker", 1L));
        assertThat(identity).isNotEqualTo(new BrokerIdentityInfo("cluster", "other-broker", 1L));
        assertThat(identity).isNotEqualTo(new BrokerIdentityInfo("cluster", "broker", 2L));
    }
}
