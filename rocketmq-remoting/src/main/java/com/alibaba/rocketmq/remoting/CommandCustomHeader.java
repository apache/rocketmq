/**
 * $Id: CommandCustomHeader.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public interface CommandCustomHeader {
    public void checkFields() throws RemotingCommandException;
}
