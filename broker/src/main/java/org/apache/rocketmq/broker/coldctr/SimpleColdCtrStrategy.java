package org.apache.rocketmq.broker.coldctr;

public class SimpleColdCtrStrategy implements ColdCtrStrategy{
    private final ColdDataCgCtrService coldDataCgCtrService;

    public SimpleColdCtrStrategy(ColdDataCgCtrService coldDataCgCtrService) {
        this.coldDataCgCtrService = coldDataCgCtrService;
    }

    @Override
    public Double decisionFactor() {
        return null;
    }

    @Override
    public void promote(String consumerGroup, Long currentThreshold) {
        coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, (long)(currentThreshold * 1.5));
    }

    @Override
    public void decelerate(String consumerGroup, Long currentThreshold) {
        if (!coldDataCgCtrService.isGlobalColdCtr()) {
            return;
        }
        long changedThresholdVal = (long)(currentThreshold * 0.8);
        if (changedThresholdVal < coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold()) {
            changedThresholdVal = coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold();
        }
        coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, changedThresholdVal);
    }

    @Override
    public void collect(Long globalAcc) {
    }
}
