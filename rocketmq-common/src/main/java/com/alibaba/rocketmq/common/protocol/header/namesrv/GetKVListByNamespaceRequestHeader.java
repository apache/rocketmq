package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class GetKVListByNamespaceRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String namespace;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getNamespace() {
        return namespace;
    }


    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
