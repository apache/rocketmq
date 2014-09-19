/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * 消息，Producer与Consumer使用
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-18
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    /**
     * 消息主题
     */
    private String topic;
    /**
     * 消息标志，系统不做干预，完全由应用决定如何使用
     */
    private int flag;
    /**
     * 消息属性，都是系统属性，禁止应用设置
     */
    private Map<String, String> properties;
    /**
     * 消息体
     */
    private byte[] body;


    public Message() {
    }


    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }


    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }


    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }


    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;

        if (tags != null && tags.length() > 0)
            this.setTags(tags);

        if (keys != null && keys.length() > 0)
            this.setKeys(keys);

        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }


    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }


    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        this.properties.put(name, value);
    }


    public void putUserProperty(final String name, final String value) {
        if (MessageConst.systemKeySet.contains(name)) {
            throw new RuntimeException(String.format(
                "The Property<%s> is used by system, input another please", name));
        }
        this.putProperty(name, value);
    }


    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }


    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        return this.properties.get(name);
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }


    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }


    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }


    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }


    public void setKeys(Collection<String> keys) {
        StringBuffer sb = new StringBuffer();
        for (String k : keys) {
            sb.append(k);
            sb.append(MessageConst.KEY_SEPARATOR);
        }

        this.setKeys(sb.toString().trim());
    }


    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }


    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }


    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result)
            return true;

        return Boolean.parseBoolean(result);
    }


    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }


    public int getFlag() {
        return flag;
    }


    public void setFlag(int flag) {
        this.flag = flag;
    }


    public byte[] getBody() {
        return body;
    }


    public void setBody(byte[] body) {
        this.body = body;
    }


    public Map<String, String> getProperties() {
        return properties;
    }


    void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }


    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }


    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }


    @Override
    public String toString() {
        return "Message [topic=" + topic + ", flag=" + flag + ", properties=" + properties + ", body="
                + (body != null ? body.length : 0) + "]";
    }
}
