/**
 * $Id: BrokerRole.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.config;

/**
 * Broker角色
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum BrokerRole {
    // 异步复制Master
    ASYNC_MASTER,
    // 同步双写Master
    SYNC_MASTER,
    // Slave
    SLAVE
}
