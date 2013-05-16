/**
 * $Id: MQClientException.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.exception;

import com.alibaba.rocketmq.common.MetaUtil;


/**
 * MQ“Ï≥£¿‡
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class MQClientException extends Exception {
    private static final long serialVersionUID = -5758410930844185841L;
    private final int responseCode;
    private final String errorMessage;


    public MQClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = -1;
        this.errorMessage = null;
    }


    public MQClientException(int responseCode, String errorMessage) {
        super("CODE: " + MetaUtil.responseCode2String(responseCode) + "\tDESC: " + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }


    public int getResponseCode() {
        return responseCode;
    }


    public String getErrorMessage() {
        return errorMessage;
    }


    public boolean needRetry() {
        // TODO
        return false;
    }
}
