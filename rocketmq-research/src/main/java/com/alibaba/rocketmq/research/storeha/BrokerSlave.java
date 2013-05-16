/**
 * $Id: BrokerSlave.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.storeha;

import com.alibaba.rocketmq.research.store.MetaStoreTestObject;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * HA≤‚ ‘
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class BrokerSlave {

    public static void main(String[] args) {
        try {
            MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

            final MetaStoreTestObject metaStoreTestObject = new MetaStoreTestObject(messageStoreConfig);

            metaStoreTestObject.updateMasterAddress("10.235.170.21:10912");

            if (!metaStoreTestObject.load()) {
                System.out.println("load store failed");
                System.exit(-1);
            }

            metaStoreTestObject.start();

            System.out.println("start OK, " + messageStoreConfig.getBrokerRole());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
