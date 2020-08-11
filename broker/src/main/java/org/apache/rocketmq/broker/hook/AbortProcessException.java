package org.apache.rocketmq.broker.hook;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

/**
 *
 * This exception is used for broker hooks only : SendMessageHook, ConsumeMessageHook
 * This exception is not ignored while executing hooks and it means that
 * certain processor should return an immediate error response to the client. The
 * error response code is included in AbortProcessException.  it's naming might
 * be confusing, so feel free to refactor this class. Also when any class implements
 * the 2 hook interface mentioned above we should be careful if we want to throw
 * an AbortProcessException, because it will change the control flow of broker
 * and cause a RemotingCommand return error immediately. So be aware of the side
 * effect before throw AbortProcessException in your implementation.
 *
 * @author youhui.zhang
 */
public class AbortProcessException extends RuntimeException {
    private static final long serialVersionUID = -5728810933841185841L;
    private int responseCode;
    private String errorMessage;

    public AbortProcessException(String errorMessage, Throwable cause) {
        super(FAQUrl.attachDefaultURL(errorMessage), cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public AbortProcessException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public AbortProcessException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
