package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


public interface RPCHook {
    public void doBeforeRequest(final RemotingCommand request);


    public void doAfterResponse(final RemotingCommand response);
}
