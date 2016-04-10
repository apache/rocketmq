/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class SendResult {
    private SendStatus sendStatus;
    private String msgId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String transactionId;
    private String offsetMsgId;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public static String encoderSendResultToJson(final Object obj) {
        return JSON.toJSONString(obj);
    }

    public static SendResult decoderSendResultFromJson(String json) {
        return JSON.parseObject(json, SendResult.class);
    }

    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public SendStatus getSendStatus() {
        return sendStatus;
    }


    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public String getTransactionId() {
        return transactionId;
    }


    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }    

    public String getOffsetMsgId() {
        return offsetMsgId;
    }
    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }
    
    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ",offsetMsgId=" + offsetMsgId + ", messageQueue=" + messageQueue
                + ", queueOffset=" + queueOffset + "]";
    }
}
