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

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;


import java.util.HashSet;
import java.util.Set;

public class HeartbeatDataTest {

    @Test
    public void test() throws Exception {
        HeartbeatData hbd = new HeartbeatData();
        Set<ProducerData> producerDataSet = new HashSet<ProducerData>();
        Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();
        
        ProducerData pd=new ProducerData();
        pd.setGroupName("testGroupName");
        producerDataSet.add(pd);
        
        ConsumerData cd=new ConsumerData();
        cd.setGroupName("testGroupName");
        cd.setUnitMode(true);
        consumerDataSet.add(cd);

        hbd.setClientID("ClientID");
        hbd.setProducerDataSet(producerDataSet);
        hbd.setConsumerDataSet(consumerDataSet);

        String json = RemotingSerializable.toJson(hbd, false);
        HeartbeatData anotherHbd = RemotingSerializable.fromJson(json, HeartbeatData.class);

        //System.out.println(anotherHbd.getConsumerDataSet().size() );
        assertThat(anotherHbd.getClientID()).isEqualTo("ClientID");
        assertThat(anotherHbd.getConsumerDataSet()).hasSize(1);
        assertThat(anotherHbd.getProducerDataSet()).hasSize(1);

    }

}
