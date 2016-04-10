package com.alibaba.rocketmq.common.protocol;

import com.alibaba.rocketmq.common.protocol.body.ConsumeStatus;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;


public class ConsumeStatusTest {

    @Test
    public void decode_test() throws Exception {
        ConsumeStatus cs = new ConsumeStatus();
        cs.setConsumeFailedTPS(0L);
        String json = RemotingSerializable.toJson(cs, true);
        System.out.println(json);
        ConsumeStatus fromJson = RemotingSerializable.fromJson(json, ConsumeStatus.class);
    }

}
