package org.apache.rocketmq.test.smoke;

import org.apache.log4j.Logger;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingOne;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import static org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils.getMappingDetailFromConfig;

@FixMethodOrder
public class StaticTopicIT extends BaseConf {

    private static Logger logger = Logger.getLogger(StaticTopicIT.class);
    private DefaultMQAdminExt defaultMQAdminExt;
    private ClientMetadata clientMetadata;

    @Before
    public void setUp() throws Exception {
        defaultMQAdminExt = getAdmin(nsAddr);
        waitBrokerRegistered(nsAddr, clusterName);
        clientMetadata = new ClientMetadata();
        defaultMQAdminExt.start();
        ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
        if (clusterInfo == null
                || clusterInfo.getClusterAddrTable().isEmpty()) {
            throw new RuntimeException("The Cluster info is empty");
        }
        clientMetadata.refreshClusterInfo(clusterInfo);
    }

    public Map<String, TopicConfigAndQueueMapping> createStaticTopic(String topic, int queueNum, Set<String> targetBrokers) throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);
        Assert.assertTrue(brokerConfigMap.isEmpty());
        TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
        Assert.assertEquals(2, brokerConfigMap.size());
        //If some succeed, and others fail, it will cause inconsistent data
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            String broker = entry.getKey();
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), false);
        }
        return brokerConfigMap;
    }

    @Test
    public void testCreateAndRemappingStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        int queueNum = 10;
        Map<String, TopicConfigAndQueueMapping> localBrokerConfigMap = createStaticTopic(topic, queueNum, getBrokers());
        {
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);
            Assert.assertEquals(2, remoteBrokerConfigMap.size());
            for (Map.Entry<String, TopicConfigAndQueueMapping> entry: remoteBrokerConfigMap.entrySet())  {
                String broker = entry.getKey();
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                TopicConfigAndQueueMapping localConfigMapping = localBrokerConfigMap.get(broker);
                Assert.assertNotNull(localConfigMapping);
                Assert.assertEquals(configMapping, localConfigMapping);
            }
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
        }
        /*{
            Set<String> targetBrokers = Collections.singleton(broker1Name);
            Map<String, TopicConfigAndQueueMapping> brokerConfigMapFromRemote = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);
            TopicRemappingDetailWrapper wrapper = TopicQueueMappingUtils.remappingStaticTopic(topic, brokerConfigMapFromRemote, targetBrokers);

        }*/
    }


    @After
    public void tearDown() {
        super.shutdown();
    }

}
