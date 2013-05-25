/**
 * $Id: PullCallback.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

/**
 * 异步拉消息回调接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface PullCallback {
    public void onSuccess(final PullResult pullResult);


    public void onException(final Throwable e);
}
