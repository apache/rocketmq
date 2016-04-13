/*
 * Copyright 2016 Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com .
 */
package com.alibaba.rocketmq.common.message;
/**
 * 类MessageClientExt.java的实现描述：客户端使用，对msgId进行了封装
 * @author yp 2016年2月26日 下午2:24:32
 */
public class MessageClientExt extends MessageExt {
        
    /**
     * 用offset msg id 取代原来的msg id
     * @param offsetMsgId
     */
    public void setOffsetMsgId(String offsetMsgId) {
        super.setMsgId(offsetMsgId);
    }
    
    /**
     * 用offset msg id 取代原来的msg id
     * @param offsetMsgId
     */
    public String getOffsetMsgId() {
        return super.getMsgId();
    }
    
    /**
     * 采用新的msgid
     */
    public void setMsgId(String msgId) {
        //DO NOTHING
        //MessageClientIDSetter.setUniqID(this);
    }
    
    /**
     * 采用新的msgid
     */
    @Override
    public String getMsgId() {
        String uniqID = MessageClientIDSetter.getUniqID(this);
        if (uniqID == null) {
            return this.getOffsetMsgId();
        }
        else {
            return uniqID;
        }
    }
}
