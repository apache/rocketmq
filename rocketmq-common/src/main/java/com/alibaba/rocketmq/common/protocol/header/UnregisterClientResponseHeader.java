/**
 *
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class UnregisterClientResponseHeader implements CommandCustomHeader {

    /*
     * (non-Javadoc)
     * 
     * @see com.alibaba.rocketmq.remoting.CommandCustomHeader#checkFields()
     */
    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }

}
