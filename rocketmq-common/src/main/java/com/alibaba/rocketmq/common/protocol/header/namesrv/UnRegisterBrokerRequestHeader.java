/**
 * $Id: UnRegisterBrokerRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author lansheng.zj@taobao.com
 */
public class UnRegisterBrokerRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String brokerName;


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }

}
