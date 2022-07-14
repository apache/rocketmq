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

package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.DefaultHAClient;
import org.apache.rocketmq.store.ha.HAClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HAClientTest {
    private HAClient haClient;

    @Mock
    private DefaultMessageStore messageStore;

    @Before
    public void setUp() throws Exception {
//        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        this.haClient = new DefaultHAClient(this.messageStore);
    }

    @After
    public void tearDown() throws Exception {
        this.haClient.shutdown();
    }

    @Test
    public void updateMasterAddress() {
        assertThat(this.haClient.getMasterAddress()).isNull();
        this.haClient.updateMasterAddress("127.0.0.1:10911");
        assertThat(this.haClient.getMasterAddress()).isEqualTo("127.0.0.1:10911");

        this.haClient.updateMasterAddress("127.0.0.1:10912");
        assertThat(this.haClient.getMasterAddress()).isEqualTo("127.0.0.1:10912");
    }

    @Test
    public void updateHaMasterAddress() {
        assertThat(this.haClient.getHaMasterAddress()).isNull();
        this.haClient.updateHaMasterAddress("127.0.0.1:10911");
        assertThat(this.haClient.getHaMasterAddress()).isEqualTo("127.0.0.1:10911");

        this.haClient.updateHaMasterAddress("127.0.0.1:10912");
        assertThat(this.haClient.getHaMasterAddress()).isEqualTo("127.0.0.1:10912");
    }
}
