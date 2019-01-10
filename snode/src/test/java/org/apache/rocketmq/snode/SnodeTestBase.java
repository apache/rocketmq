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
package org.apache.rocketmq.snode;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class SnodeTestBase {

    public SendMessageRequestHeaderV2 createSendMsgRequestHeader(String group, String topic) {
        SendMessageRequestHeaderV2 requestHeader = new SendMessageRequestHeaderV2();
        requestHeader.setA(group);
        requestHeader.setB(topic);
        requestHeader.setC(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setD(3);
        requestHeader.setE(1);
        requestHeader.setF(0);
        requestHeader.setG(System.currentTimeMillis());
        requestHeader.setH(124);
        return requestHeader;
    }

    public RemotingCommand createSendMesssageCommand(String group, String topic) {
        SendMessageRequestHeaderV2 requestHeader = createSendMsgRequestHeader(group, topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeader);
        request.setBody(new byte[] {'a'});
        return request;
    }

    public RemotingCommand createSuccessResponse() {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(1234);

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        responseHeader.setMsgId("123");
        responseHeader.setQueueId(1);
        responseHeader.setQueueOffset(123L);

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, "RegionHZ");
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, "true");
        response.addExtField("queueId", String.valueOf(responseHeader.getQueueId()));
        response.addExtField("msgId", responseHeader.getMsgId());
        response.addExtField("queueOffset", String.valueOf(responseHeader.getQueueOffset()));
        return response;
    }
}
