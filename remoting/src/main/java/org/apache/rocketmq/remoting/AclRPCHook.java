package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AclRPCHook implements RPCHook {

    private static final String ROCKETMQ_USER="rocketmq.user";
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

        String userName=System.getProperty(ROCKETMQ_USER,"");
        request.addExtField(ROCKETMQ_USER,userName);

    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
                                RemotingCommand response) {

    }
}
