/**
 * $Id: FlushDiskType.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.config;

/**
 * 刷盘方式
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public enum FlushDiskType {
    /**
     * 同步刷盘
     */
    SYNC_FLUSH,
    /**
     * 异步刷盘
     */
    ASYNC_FLUSH
}
