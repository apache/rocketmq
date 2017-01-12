package org.apache.rocketmq.remoting;


import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.junit.Test;

import java.util.List;

/**
 * Created by tain on 2017/1/11.
 */
public class LocalAddressTest {
    @Test
    public void getLoacalAddress()
    {
        List address =  RemotingUtil.getLocalAddressArray();
        System.out.println(address);
    }
}
