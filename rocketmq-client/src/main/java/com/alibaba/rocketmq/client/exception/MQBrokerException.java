/**
 * $Id: MQBrokerException.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.exception;

import com.alibaba.rocketmq.common.UtilALl;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQBrokerException extends Exception {
    private static final long serialVersionUID = 5975020272601250368L;
    private final int responseCode;
    private final String errorMessage;


    public MQBrokerException(int responseCode, String errorMessage) {
        super("CODE: " + UtilALl.responseCode2String(responseCode) + "\tDESC: " + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }


    public int getResponseCode() {
        return responseCode;
    }


    public String getErrorMessage() {
        return errorMessage;
    }

    //
    // public boolean needRetry() {
    // // TODO
    // return false;
    // }
}
