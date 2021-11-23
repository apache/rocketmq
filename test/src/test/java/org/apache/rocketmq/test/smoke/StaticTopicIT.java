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
        ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
        if (clusterInfo == null
                || clusterInfo.getClusterAddrTable().isEmpty()) {
            throw new RuntimeException("The Cluster info is empty");
        }
        clientMetadata.refreshClusterInfo(clusterInfo);

    }

    @Test
    public void testCreateStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        int queueNum = 10;
        Set<String> brokers = getBrokers();
        //create topic
        {
            Map<String, TopicConfigAndQueueMapping> brokerConfigMap = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);
            Assert.assertTrue(brokerConfigMap.isEmpty());
            TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, getBrokers(), brokerConfigMap);
            //If some succeed, and others fail, it will cause inconsistent data
            for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
                String broker = entry.getKey();
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), false);
            }
        }
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);

        TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
        Assert.assertEquals(queueNum, globalIdMap.size());

    }

    @After
    public void tearDown() {
        super.shutdown();
    }

}
