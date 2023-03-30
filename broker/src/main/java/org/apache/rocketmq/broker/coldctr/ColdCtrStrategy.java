package org.apache.rocketmq.broker.coldctr;

public interface ColdCtrStrategy {
    Double decisionFactor();

    void promote(String consumerGroup, Long currentThreshold);

    void decelerate(String consumerGroup, Long currentThreshold);

    void collect(Long globalAcc);
}
