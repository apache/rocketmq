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

package org.apache.rocketmq.remoting.protocol.header;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SendMessageRequestHeaderV2Test {
    SendMessageRequestHeaderV2 header = new SendMessageRequestHeaderV2();
    String topic = "test";
    int queueId = 5;

    @Test
    public void testEncode() {
        header.setQueueId(queueId);
        header.setTopic(topic);
        assertThat(JSON.toJSONString(header)).isEqualTo("{\"b\":\"test\",\"e\":5}");
    }

    @Test
    public void testDecode() {
        header.setQueueId(queueId);
        header.setTopic(topic);
        assertThat(header).isEqualTo(JSON.parseObject("{\"b\":\"test\",\"e\":5}", SendMessageRequestHeaderV2.class));
    }
}