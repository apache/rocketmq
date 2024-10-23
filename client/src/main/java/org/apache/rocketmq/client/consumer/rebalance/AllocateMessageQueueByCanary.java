package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.canary.CanaryAlgorithm;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.constant.CanaryConstants;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * canary deploy mode queue algorithm
 *
 * @author xiejr
 * @since 2024/7/5 11:39 AM
 */
public class AllocateMessageQueueByCanary extends AbstractAllocateMessageQueueStrategy {

    private static final Logger log = LoggerFactory.getLogger(AllocateMessageQueueByCanary.class);


    private final CanaryAlgorithm canaryAlgorithm;


    private final AllocateMessageQueueStrategy delegate;

    public AllocateMessageQueueByCanary(CanaryAlgorithm canaryAlgorithm, AllocateMessageQueueStrategy delegate) {
        this.canaryAlgorithm = canaryAlgorithm;
        this.delegate = delegate == null ? new AllocateMessageQueueAveragely() : delegate;
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        log.debug("[canary rebalance]: consumerGroup: {}, currentCID: {}, mqAll: {}, cidAll: {}", consumerGroup, currentCID, mqAll, cidAll);
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return Collections.emptyList();
        }
        List<String> canaryClients = cidAll.stream().filter(this::isCanaryClient).collect(Collectors.toList());
        if (canaryClients.size() == cidAll.size() || canaryClients.size() == 0) {
            log.debug("[canary rebalance]: all clients are canary or not, maybe the new subscription or default client, use the default rebalance strategy");
            return delegate.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }
        cidAll.removeAll(canaryClients);
        List<MessageQueue> canaryQueues = canaryAlgorithm.getCanaryQueues(mqAll);
        mqAll.removeAll(canaryQueues);
        Collections.sort(cidAll);
        Collections.sort(canaryClients);
        Collections.sort(canaryQueues);
        Collections.sort(canaryQueues);
        if (canaryClients.contains(currentCID)) {
            log.debug("[canary rebalance]: current client is canary, allocate canary queues");
            return delegate.allocate(consumerGroup, currentCID, canaryQueues, canaryClients);
        } else {
            return delegate.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }
    }


    private boolean isCanaryClient(String clientId) {
        int i = clientId.lastIndexOf("@");
        if (-1 == i) {
            return false;
        }
        return CanaryConstants.CANARY_TAG.equals(clientId.substring(i + 1));
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
