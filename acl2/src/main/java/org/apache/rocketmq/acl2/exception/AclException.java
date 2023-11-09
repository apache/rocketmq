package org.apache.rocketmq.acl2.exception;

import org.slf4j.helpers.MessageFormatter;

public class AclException extends RuntimeException {

    private final int code;

    public AclException(int code, String message) {
        super(message);
        this.code = code;
    }

    public AclException(int code, String messagePattern, Object... argArray) {
        super(MessageFormatter.arrayFormat(messagePattern, argArray).getMessage());
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
