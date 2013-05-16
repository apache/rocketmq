/**
 * $Id: SendCallback.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

/**
 * 异步发送消息回调接口
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public interface SendCallback {
    public void onSuccess(final SendResult sendResult);


    public void onException(final Throwable e);
}
