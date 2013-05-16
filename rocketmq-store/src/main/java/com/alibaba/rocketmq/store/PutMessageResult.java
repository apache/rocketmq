/**
 * $Id: PutMessageResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

/**
 * 写入消息返回结果
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class PutMessageResult {
    private PutMessageStatus putMessageStatus;
    private AppendMessageResult appendMessageResult;


    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }


    public boolean isOk() {
        return this.appendMessageResult.isOk();
    }


    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }


    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }


    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }


    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }
}
