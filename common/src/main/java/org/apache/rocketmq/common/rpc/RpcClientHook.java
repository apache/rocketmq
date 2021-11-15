package org.apache.rocketmq.common.rpc;

import java.util.concurrent.Future;

public abstract class RpcClientHook {

    //if the return is not null, return it
    public abstract RpcResponse beforeRequest(RpcRequest rpcRequest) throws RpcException;

    //if the return is not null, return it
    public abstract RpcResponse afterResponse(RpcResponse rpcResponse) throws RpcException;

}
