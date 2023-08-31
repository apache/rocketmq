package org.apache.rocketmq.broker.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.junit.Test;

public class NetworkUtilTest {

    @Test
    public void validateNamesrvAddress() throws Exception {
        String namesrvAddr = "192.125.36.2:9876;localhost:9876";
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.validateNamesrvAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                    "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }
    }
}
