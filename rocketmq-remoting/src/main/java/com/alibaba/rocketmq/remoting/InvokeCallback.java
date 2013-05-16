/**
 * $Id: InvokeCallback.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.netty.ResponseFuture;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public interface InvokeCallback {
    public void operationComplete(final ResponseFuture responseFuture);
}
