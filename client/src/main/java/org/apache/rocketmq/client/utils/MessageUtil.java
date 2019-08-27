package org.apache.rocketmq.client.utils;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;

public class MessageUtil {
    public static Message createReplyMessage(final Message requestMessage) {
        if (requestMessage != null) {
            Message replyMessage = new Message();
            String cluster = requestMessage.getProperty(MessageConst.PROPERTY_CLUSTER);
            String replyTo = requestMessage.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO);
            String requestUniqId = requestMessage.getProperty(MessageConst.PROPERTY_REQUEST_UNIQ_ID);
            String ttl = requestMessage.getProperty(MessageConst.PROPERTY_MESSAGE_TTL);
            if (cluster == null) {

            }
            String replyTopic = MixAll.getReplyTopic(cluster);
            replyMessage.setTopic(replyTopic);
            MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
            MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_REQUEST_UNIQ_ID, requestUniqId);
            MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_REPLY_TO, replyTo);
            MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_TTL, ttl);

            return replyMessage;
        }
        return null;
    }
}
