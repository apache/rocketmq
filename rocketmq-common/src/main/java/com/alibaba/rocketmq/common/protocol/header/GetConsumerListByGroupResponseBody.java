package com.alibaba.rocketmq.common.protocol.header;

import java.util.List;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class GetConsumerListByGroupResponseBody extends RemotingSerializable {
    private List<String> consumerIdList;


    public List<String> getConsumerIdList() {
        return consumerIdList;
    }


    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }
}
