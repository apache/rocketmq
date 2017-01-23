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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class ConsumeStatusTest {

    @Test
    public void testFromJson() throws Exception {
        ConsumeStatus cs = new ConsumeStatus();
        cs.setConsumeFailedTPS(10);
        cs.setPullRT(100);
        cs.setPullTPS(1000);
        String json = RemotingSerializable.toJson(cs, true);
        ConsumeStatus fromJson = RemotingSerializable.fromJson(json, ConsumeStatus.class);
        assertThat(fromJson.getPullRT()).isCloseTo(cs.getPullRT(), within(0.0001));
        assertThat(fromJson.getPullTPS()).isCloseTo(cs.getPullTPS(), within(0.0001));
        assertThat(fromJson.getConsumeFailedTPS()).isCloseTo(cs.getConsumeFailedTPS(), within(0.0001));
    }

}
