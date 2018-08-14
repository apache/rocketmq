package org.apache.rocketmq.hook.exception;

public class AclFailException extends RuntimeException {

    public AclFailException() {
    }

    public AclFailException(String message) {
        super(message);
    }

    public AclFailException(String message, Throwable cause) {
        super(message, cause);
    }

    public AclFailException(Throwable cause) {
        super(cause);
    }

    public AclFailException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}