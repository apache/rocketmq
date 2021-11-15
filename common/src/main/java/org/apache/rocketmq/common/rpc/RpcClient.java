package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.concurrent.Future;

public interface RpcClient {


    //common invoke paradigm, the logic remote addr is defined in "bname" field of request
    //For oneway request, the sign is labeled in request, and do not need an another method named "invokeOneway"
    //For one
    Future<RpcResponse>  invoke(RpcRequest request, long timeoutMs) throws RpcException;

    //For rocketmq, most requests are corresponded to MessageQueue
    //And for LogicQueue, the broker name is mocked, the physical addr could only be defined by MessageQueue
    Future<RpcResponse>  invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException;

}
