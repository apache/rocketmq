package com.alibaba.rocketmq.common;

import java.net.InetAddress;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;


/**
 * @auther lansheng.zj@taobao.com
 */
public class MixAllTest {

    @Test
    public void test() throws Exception {
        List<String> localInetAddress = MixAll.getLocalInetAddress();
        String local = InetAddress.getLocalHost().getHostAddress();
        Assert.assertTrue(localInetAddress.contains("127.0.0.1"));
        Assert.assertTrue(localInetAddress.contains(local));
    }
}
