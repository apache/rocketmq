package org.apache.rocketmq.auth.authentication.exception;

import org.slf4j.helpers.MessageFormatter;

public class AuthenticationException extends RuntimeException {
    private int code;

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthenticationException(int code, String message) {
        super(message);
        this.code = code;
    }

    public AuthenticationException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public AuthenticationException(String messagePattern, Object... argArray) {
        super(MessageFormatter.arrayFormat(messagePattern, argArray).getMessage());
    }

    public int getCode() {
        return code;
    }
}
