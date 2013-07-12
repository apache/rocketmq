package com.alibaba.rocketmq.broker.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;


/**
 * Slave从Master同步信息（非消息）
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-8
 */
public class SlaveSynchronize {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;


    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }


    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
    }


    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                TopicConfigSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("update slave topic config from master, {}", masterAddrBak);
                }
            }
            catch (Exception e) {
                log.error("syncTopicConfig Exception, " + masterAddrBak, e);
            }
        }
    }


    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {

        }
    }


    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {

        }
    }
}
