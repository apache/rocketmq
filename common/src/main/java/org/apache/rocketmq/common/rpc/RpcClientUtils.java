package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.nio.ByteBuffer;

public class RpcClientUtils {

    public static RemotingCommand createCommandForRpcRequest(RpcRequest rpcRequest) {
        RemotingCommand cmd = RemotingCommand.createRequestCommand(rpcRequest.getCode(), rpcRequest.getHeader());
        cmd.setBody(encodeBody(rpcRequest.getBody()));
        return cmd;
    }

    public static RemotingCommand createCommandForRpcResponse(RpcResponse rpcResponse) {
        RemotingCommand cmd = RemotingCommand.createResponseCommand(rpcResponse.getCode(), rpcResponse.getHeader());
        cmd.setRemark(rpcResponse.getException() == null ? "" : rpcResponse.getException().getMessage());
        cmd.setBody(encodeBody(rpcResponse.getBody()));
        return cmd;
    }

    public static byte[] encodeBody(Object body) {
        if (body instanceof byte[]) {
            return (byte[])body;
        } else if (body instanceof RemotingSerializable) {
            return ((RemotingSerializable) body).encode();
        } else if (body instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer)body;
            buffer.mark();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            buffer.reset();
            return data;
        } else {
            throw new RuntimeException("Unsupported body type " + body.getClass());
        }
    }
}
