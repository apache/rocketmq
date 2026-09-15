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
package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson2.JSON;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class PopCheckPointTest {

    @Test
    public void testToJsonBytesMatchesJsonStringBytes() {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setTopic("topic-\u4e2d\u6587");
        ck.setCId("group");
        ck.setQueueId(3);
        ck.setStartOffset(200L);
        ck.setPopTime(1670212915531L);
        ck.setInvisibleTime(60000L);
        ck.setBitMap(5);
        ck.setNum((byte) 2);
        ck.setBrokerName("broker-a");
        ck.addDiff(1);
        ck.addDiff(3);
        ck.setRePutTimes("1");

        byte[] direct = JSON.toJSONBytes(ck);
        Assert.assertArrayEquals(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8), direct);

        PopCheckPoint decoded = JSON.parseObject(direct, PopCheckPoint.class);
        Assert.assertEquals(ck.getTopic(), decoded.getTopic());
        Assert.assertEquals(ck.getCId(), decoded.getCId());
        Assert.assertEquals(ck.getStartOffset(), decoded.getStartOffset());
        Assert.assertEquals(ck.getPopTime(), decoded.getPopTime());
        Assert.assertEquals(ck.getQueueOffsetDiff(), decoded.getQueueOffsetDiff());
        Assert.assertEquals(ck.getBitMap(), decoded.getBitMap());
    }
}
