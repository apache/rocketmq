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

import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetLiteTopicListRequestHeaderTest {

    @Test
    public void testEncodeAndDecode() throws RemotingCommandException {
        GetLiteTopicListRequestHeader header = new GetLiteTopicListRequestHeader();
        header.setParentTopic("parent");
        header.setPrefix("order-");
        header.setMaxCount(32);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_TOPIC_LIST, header);
        request.makeCustomHeaderToNet();

        GetLiteTopicListRequestHeader decoded =
            request.decodeCommandCustomHeader(GetLiteTopicListRequestHeader.class);
        assertEquals("parent", decoded.getParentTopic());
        assertEquals("order-", decoded.getPrefix());
        assertEquals(32, decoded.getMaxCount());
    }

    @Test
    public void testDefaultMaxCount() {
        GetLiteTopicListRequestHeader header = new GetLiteTopicListRequestHeader();
        assertEquals(1000, header.getMaxCount());
    }

    @Test
    public void testCheckFieldsRejectsNonPositiveMaxCount() throws RemotingCommandException {
        GetLiteTopicListRequestHeader header = new GetLiteTopicListRequestHeader();
        header.setMaxCount(0);
        try {
            header.checkFields();
            fail("expected RemotingCommandException");
        } catch (RemotingCommandException expected) {
            assertEquals("[maxCount] field invalid", expected.getMessage());
        }

        header.setMaxCount(-1);
        try {
            header.checkFields();
            fail("expected RemotingCommandException");
        } catch (RemotingCommandException expected) {
            assertEquals("[maxCount] field invalid", expected.getMessage());
        }
    }
}
