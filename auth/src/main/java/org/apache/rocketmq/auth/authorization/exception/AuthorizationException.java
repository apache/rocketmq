package org.apache.rocketmq.auth.authorization.exception;

import org.slf4j.helpers.MessageFormatter;

public class AuthorizationException extends RuntimeException {

    private int code;

    public AuthorizationException(String message) {
        super(message);
    }

    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthorizationException(int code, String message) {
        super(message);
        this.code = code;
    }

    public AuthorizationException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public AuthorizationException(String messagePattern, Object... argArray) {
        super(MessageFormatter.arrayFormat(messagePattern, argArray).getMessage());
    }

    public int getCode() {
        return code;
    }
}
