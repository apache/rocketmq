package org.apache.rocketmq.store.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class MultiDispatch {

    public static String lmqQueueKey(String queueName) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = 0;
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    public static boolean isNeedHandleMultiDispatch(MessageStoreConfig messageStoreConfig, String topic) {
        return messageStoreConfig.isEnableMultiDispatch()
            && !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
            && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public static boolean checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, DispatchRequest dispatchRequest) {
        if (!isNeedHandleMultiDispatch(messageStoreConfig, dispatchRequest.getTopic())) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    public static List<DispatchRequest> checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, List<DispatchRequest> dispatchRequests) {
        if (!messageStoreConfig.isEnableMultiDispatch() || dispatchRequests == null || dispatchRequests.size() == 0) {
            return null;
        }
        List<DispatchRequest> result = new ArrayList<>();
        for (DispatchRequest dispatchRequest : dispatchRequests) {
            if (checkMultiDispatchQueue(messageStoreConfig, dispatchRequest)) {
                result.add(dispatchRequest);
            }
        }
        return dispatchRequests;
    }
}
