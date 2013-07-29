/**
 * $Id: RPCProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc;

import java.nio.ByteBuffer;


/**
 * Server与Client的读事件处理
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface RPCProcessor {
    public byte[] process(final int upId, final ByteBuffer upstream);
}
