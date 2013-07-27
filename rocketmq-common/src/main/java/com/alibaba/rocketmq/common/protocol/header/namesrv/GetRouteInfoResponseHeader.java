/**
 * $Id: GetRouteInfoResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class GetRouteInfoResponseHeader implements CommandCustomHeader {

    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }
}
