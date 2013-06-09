/**
 * $Id: MessageExtBrokerInner.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class MessageExtBrokerInner extends MessageExt {
    private String propertiesString;
    private long tagsCode;


    public String getPropertiesString() {
        return propertiesString;
    }


    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }


    public long getTagsCode() {
        return tagsCode;
    }


    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }


    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.length() == 0)
            return 0;
        // TODO
        return 0;
    }
}
