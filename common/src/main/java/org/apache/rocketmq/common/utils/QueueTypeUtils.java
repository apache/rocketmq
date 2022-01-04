package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QueueTypeUtils {

    public static boolean isBatchCq(Optional<TopicConfig> topicConfig) {
        return Objects.equals(CQType.BatchCQ, getCQType(topicConfig));
    }

    public static CQType getCQType(Optional<TopicConfig> topicConfig) {
        if (!topicConfig.isPresent()) {
            return CQType.valueOf(TopicAttributes.queueType.getDefaultValue());
        }

        String attributeName = TopicAttributes.queueType.getName();

        Map<String, String> attributes = topicConfig.get().getAttributes();
        if (attributes == null || attributes.size() == 0) {
            return CQType.valueOf(TopicAttributes.queueType.getDefaultValue());
        }

        if (attributes.containsKey(attributeName)) {
            return CQType.valueOf(attributes.get(attributeName));
        } else {
            return CQType.valueOf(TopicAttributes.queueType.getDefaultValue());
        }
    }
}