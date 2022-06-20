package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AbstractRpcHook implements RPCHook {

    private RPCHookContext handlerContext;

    @Override public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

    }

    @Override public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }

    //This method should move to the parent interface RPCHook in the future
    //Currently, to be compatible with client of jdk 1.6, put it here
    public RPCHookContext getContext() {
        return handlerContext;
    }

    //This method should move to the parent interface RPCHook in the future
    //Currently, to be compatible with client of jdk 1.6, put it here
    public void setContext(RPCHookContext handlerContext) {
        this.handlerContext = handlerContext;
    }
}
