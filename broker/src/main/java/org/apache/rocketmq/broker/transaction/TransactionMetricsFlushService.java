package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TransactionMetricsFlushService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private BrokerController brokerController;
    public TransactionMetricsFlushService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return "TransactionFlushService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service start");
        long start = System.currentTimeMillis();
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() - start > brokerController.getBrokerConfig().getTransactionMetricFlushInterval()) {
                    start = System.currentTimeMillis();
                    brokerController.getTransactionalMessageService().getTransactionMetrics().persist();
                    waitForRunning(brokerController.getBrokerConfig().getTransactionMetricFlushInterval());
                }
            } catch (Throwable e) {
                log.error("Error occurred in " + getServiceName(), e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }
}