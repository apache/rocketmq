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

package org.apache.rocketmq.common.protocol.body;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetSerializeWrapperTest {

    @Test
    public void testFromJson() {
        ConsumerOffsetSerializeWrapper consumerOffsetSerializeWrapper = new ConsumerOffsetSerializeWrapper();
        String offsetTableString = "offsetTable";
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable =
                new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>();
        ConcurrentMap<Integer, Long> integerLongConcurrentMap = new ConcurrentHashMap<Integer, Long>();
        integerLongConcurrentMap.put(1, 2L);
        offsetTable.put(offsetTableString, integerLongConcurrentMap);
        consumerOffsetSerializeWrapper.setOffsetTable(offsetTable);
        String json = RemotingSerializable.toJson(consumerOffsetSerializeWrapper, true);
        ConsumerOffsetSerializeWrapper fromJson = RemotingSerializable.fromJson(json, ConsumerOffsetSerializeWrapper.class);
        assertThat(fromJson).isNotNull();
        assertThat(fromJson.getOffsetTable()).isEqualTo(consumerOffsetSerializeWrapper.getOffsetTable());
        assertThat(fromJson.getOffsetTable().get(offsetTableString)).isEqualTo(consumerOffsetSerializeWrapper.getOffsetTable().get(offsetTableString));
    }
}
