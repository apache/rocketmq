package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class GetKVConfigResponseHeader implements CommandCustomHeader {
    @CFNullable
    private String value;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getValue() {
        return value;
    }


    public void setValue(String value) {
        this.value = value;
    }
}
