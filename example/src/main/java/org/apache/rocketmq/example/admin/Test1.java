package org.apache.rocketmq.example.admin;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;


/**
 * Created by zhangshuqing on 2018/7/1.
 */
public class Test1 {

    public static void main(String[] args) throws Exception{
        //showTopicWhatGroupSub();
        listAllGroup();
    }

    //消费组订阅对应topic   消费组为Consumer的集合名称
    private static void groupSubTopic() throws Exception{
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=getDefaultMQAdminExt();
        defaultMQAdminExt.start();

        //消费组绑定topic
    }

    //显示所有gruop
    private static void listAllGroup() throws Exception{
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=getDefaultMQAdminExt();
        defaultMQAdminExt.start();
        String brokerAddr="119.29.8.106:10911";
        SubscriptionGroupWrapper wrapper=defaultMQAdminExt.getAllSubscriptionGroup(brokerAddr,5000);
        for (Map.Entry<String, SubscriptionGroupConfig> stringSubscriptionGroupConfigEntry : wrapper.getSubscriptionGroupTable().entrySet()) {
            String key=stringSubscriptionGroupConfigEntry.getKey();
            SubscriptionGroupConfig subscriptionGroupConfig=stringSubscriptionGroupConfigEntry.getValue();
            String brokerName=subscriptionGroupConfig.getGroupName();
            System.out.println("brokerName:"+brokerName);
        }
        defaultMQAdminExt.shutdown();

    }

    //显示topic 订阅的group
    private static void showTopicWhatGroupSub() throws Exception{
        Properties systemProperties=System.getProperties();
        systemProperties.setProperty("rocketmq.namesrv.addr","119.29.8.106:9876");
        DefaultMQAdminExt defaultMQAdminExt=getDefaultMQAdminExt();
        defaultMQAdminExt.start();

        TopicList topicList=defaultMQAdminExt.fetchTopicsByCLuster("DefaultCluster");
        Set<String> topicSet=topicList.getTopicList();
        for (String topicName : topicSet) {
            //查找topic对应的group
            System.out.println("topic:"+topicName);
            GroupList groupList=defaultMQAdminExt.queryTopicConsumeByWho(topicName);
            for (String s : groupList.getGroupList()) {
                System.out.println("topic["+topicName+"]被["+s+"]消费");
            }
        }
        defaultMQAdminExt.shutdown();
    }

    /**
     * 根据groupName 获取group下面所订阅的topic
     * @param groupName
     */
    private static void querysubTopicByGroup(String groupName){

    }

    protected static DefaultMQAdminExt getDefaultMQAdminExt(){
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis())+new Random(100000));
        return defaultMQAdminExt;
    }
}
