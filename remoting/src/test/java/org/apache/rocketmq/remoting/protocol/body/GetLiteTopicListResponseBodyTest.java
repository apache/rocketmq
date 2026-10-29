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

package org.apache.rocketmq.remoting.protocol.body;

import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetLiteTopicListResponseBodyTest {

    @Test
    public void testEncodeAndDecode() {
        GetLiteTopicListResponseBody body = new GetLiteTopicListResponseBody();
        body.setParentTopic("parent");
        body.setTotal(2);
        body.setHasMore(true);

        GetLiteTopicListResponseBody.LiteTopicInfo first = new GetLiteTopicListResponseBody.LiteTopicInfo();
        first.setLiteTopic("order-1");
        first.setMinOffset(1);
        first.setMaxOffset(9);
        first.setLastUpdateTimestamp(123456L);
        first.setSubscriberCount(3);
        first.setShardingToBroker(true);

        GetLiteTopicListResponseBody.LiteTopicInfo second = new GetLiteTopicListResponseBody.LiteTopicInfo();
        second.setLiteTopic("order-2");
        second.setMinOffset(0);
        second.setMaxOffset(4);
        second.setLastUpdateTimestamp(654321L);
        second.setSubscriberCount(1);
        second.setShardingToBroker(false);
        body.setLiteTopicList(Arrays.asList(first, second));

        GetLiteTopicListResponseBody decoded =
            GetLiteTopicListResponseBody.decode(body.encode(), GetLiteTopicListResponseBody.class);

        assertEquals("parent", decoded.getParentTopic());
        assertEquals(2, decoded.getTotal());
        assertTrue(decoded.isHasMore());
        assertEquals(2, decoded.getLiteTopicList().size());
        GetLiteTopicListResponseBody.LiteTopicInfo decodedFirst = decoded.getLiteTopicList().get(0);
        assertEquals("order-1", decodedFirst.getLiteTopic());
        assertEquals(1L, decodedFirst.getMinOffset());
        assertEquals(9L, decodedFirst.getMaxOffset());
        assertEquals(123456L, decodedFirst.getLastUpdateTimestamp());
        assertEquals(3, decodedFirst.getSubscriberCount());
        assertTrue(decodedFirst.isShardingToBroker());
        assertFalse(decoded.getLiteTopicList().get(1).isShardingToBroker());
    }
}
