/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
