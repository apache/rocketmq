package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.remoting.exception.RemotingException;

public class RpcException extends RemotingException {
    private int errorCode;
    public RpcException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RpcException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
