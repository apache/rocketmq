/**
 * $Id: MQClientException.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.exception;

import com.alibaba.rocketmq.common.UtilALl;
import com.alibaba.rocketmq.common.help.FAQUrl;


/**
 * MQ“Ï≥£¿‡
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQClientException extends Exception {
    private static final long serialVersionUID = -5758410930844185841L;
    private final int responseCode;
    private final String errorMessage;


    public MQClientException(String errorMessage, Throwable cause) {
        super(FAQUrl.attachDefaultURL(errorMessage), cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }


    public MQClientException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilALl.responseCode2String(responseCode) + "  DESC: "
                + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }


    public int getResponseCode() {
        return responseCode;
    }


    public String getErrorMessage() {
        return errorMessage;
    }
}
