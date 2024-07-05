package org.apache.rocketmq.client.canary;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * The {@code CanaryAlgorithm} interface provides additional functionality for canary deployment strategies
 * <p>Canary deployment is a technique to reduce the risk of introducing a new
 * software version in production by gradually rolling out the change to a small
 * subset of users before making it available to everyone.</p>
 *
 * @author xiejr
 * @since 2024/7/5 3:37 PM
 */
public interface CanaryAlgorithm extends MessageQueueSelector {

    /**
     * Checks if the current instance is operating in canary mode.
     *
     * <p>In canary mode, only a subset of message queues or consumers will be
     * selected to receive messages, allowing for controlled testing and monitoring
     * of new features or changes before full deployment.</p>
     *
     * @return {@code true} if the instance is in canary mode; {@code false} otherwise.
     */
    boolean isCanary();


    /**
     * Filters out the canary (gray release) queues from the given list of message queues.
     * The implementation details of how to determine if a queue is a canary queue
     * are left to the user of this method.
     *
     * @param queues the list of message queues to filter
     * @return a list of message queues that are identified as canary queues
     */
    List<MessageQueue> getCanaryQueues(List<MessageQueue> queues);
}
