package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public final class MsgUtil {
    private MsgUtil() {
    }

    public static void setMessageDeliverTime(BrokerController brokerController, Message msgInner, long timeMillis) {
        msgInner.setDelayTimeLevel(brokerController.getMessageStore().getScheduleMessageService().computeDelayLevel(timeMillis));
    }

    public static long getMessageDeliverTime(BrokerController brokerController, MessageExt msgInner) {
        return brokerController.getMessageStore().getScheduleMessageService().computeDeliverTimestamp(msgInner.getDelayTimeLevel(), msgInner.getStoreTimestamp());
    }
}
