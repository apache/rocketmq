package org.apache.rocketmq.example.admin;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangshuqing on 2018/6/30.
 */
public class DefaultMQAdminExtTest {

    public static void main(String[] args) throws Exception {
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=new DefaultMQAdminExt();
        defaultMQAdminExt.start();

        ClusterInfo clusterInfo=defaultMQAdminExt.examineBrokerClusterInfo();



        HashMap<String,Set<String>> clusterAddrMap=clusterInfo.getClusterAddrTable();

        System.out.println("clusterInfo===>"+JSON.toJSONString(clusterAddrMap));

        //createTopic(defaultMQAdminExt,"defaultMQAdiminExtTopic1");

        TopicList topicList=defaultMQAdminExt.fetchTopicsByCLuster("DefaultCluster");
        Set<String> topicSet=topicList.getTopicList();
        System.out.println("TopicSet===>"+JSON.toJSONString(topicSet));

        createConsumerGroup();
//        resetOffsetMsg(-8,defaultMQAdminExt);
        defaultMQAdminExt.shutdown();

    }

    private static void showbrokerInfo( ClusterInfo clusterInfo,DefaultMQAdminExt defaultMQAdminExt) throws Exception{
        HashMap<String,BrokerData> brokerMap=clusterInfo.getBrokerAddrTable();
        Set<String> keys=brokerMap.keySet();
        Iterator<String> iterator=keys.iterator();
        while (iterator.hasNext()){
            String key=iterator.next();
            BrokerData brokerData=brokerMap.get(key);
            HashMap<Long,String> brokerAddr=brokerData.getBrokerAddrs();
            Set<Map.Entry<Long,String>> entrySet=brokerAddr.entrySet();
            for (Map.Entry<Long, String> longStringEntry : entrySet) {
                Long k=longStringEntry.getKey();
                String v=longStringEntry.getValue();
                System.out.println("broker key:"+k+":broker value:"+v);

                TopicConfigSerializeWrapper topicConfigSerializeWrapper=defaultMQAdminExt.getAllTopicGroup(v,1000);
                ConcurrentMap<String,TopicConfig > topicConfigMap=topicConfigSerializeWrapper.getTopicConfigTable();
                for (Map.Entry<String, TopicConfig> stringTopicConfigEntry : topicConfigMap.entrySet()) {
                    String kk=stringTopicConfigEntry.getKey();
                    TopicConfig vv=stringTopicConfigEntry.getValue();
                    System.out.println(kk+"==="+vv);
                    System.out.println("----------------------------");
                }
            }
        }

    }

    private static void resetOffsetMsg(int steep,DefaultMQAdminExt defaultMQAdminExt) throws Exception{
        //消息回溯 topic,group,timestamp-回溯时间,isForce-强制
        Date offesettime= DateUtils.addDays(new Date(),steep);//回溯steep天前的数据
        defaultMQAdminExt.resetOffsetByTimestamp("TopicTest","TestRocketMq",offesettime.getTime(),true);

    }


    //创建topic 绑定到指定的broker
    private static void createTopic(DefaultMQAdminExt defaultMQAdminExt,String topicName) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //这里要指定ip和端口
        String brokerAddr="119.29.8.106:10911";
        TopicConfig topicConfig=new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setOrder(false);//是否循序读写
//        topicConfig.setPerm();//读写权限
        topicConfig.setReadQueueNums(1000);//读队列数量
        topicConfig.setWriteQueueNums(1000);//写队列数量
//        topicConfig.setTopicFilterType();

        defaultMQAdminExt.createAndUpdateTopicConfig(brokerAddr,topicConfig);
    }

    //创建消费组
    private static void createConsumerGroup() throws Exception {
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=getDefaultMQAdminExt();
        defaultMQAdminExt.start();

        //创建消费组 Consumer的集合名称
        String masterAddr="119.29.8.106:10911";//broker地址
        SubscriptionGroupConfig subscriptionGroupConfig=new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("consumerGroup1");
        //broker注册消费组
        defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(masterAddr,subscriptionGroupConfig);


    }



    //订阅topic
    private static void gropuSubTopic() throws Exception{
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=getDefaultMQAdminExt();
        defaultMQAdminExt.start();
    }


    protected static DefaultMQAdminExt getDefaultMQAdminExt(){
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis())+new Random(100000));
        return defaultMQAdminExt;
    }

}
