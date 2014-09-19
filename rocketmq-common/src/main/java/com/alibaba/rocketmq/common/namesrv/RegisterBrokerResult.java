package com.alibaba.rocketmq.common.namesrv;

import com.alibaba.rocketmq.common.protocol.body.KVTable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-8
 */
public class RegisterBrokerResult {
    private String haServerAddr;
    private String masterAddr;
    private KVTable kvTable;


    public String getHaServerAddr() {
        return haServerAddr;
    }


    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }


    public KVTable getKvTable() {
        return kvTable;
    }


    public void setKvTable(KVTable kvTable) {
        this.kvTable = kvTable;
    }
}
