package com.alibaba.research.fastjson;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;


/**
 * 测试equal方法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 13-7-29
 */
public class TestEqual {

    @Test
    public void test_equal() {
        TopicRouteData data1 = this.buildTopicRouteData();
        TopicRouteData data2 = this.buildTopicRouteData2();

        assertTrue(data1.equals(data2));
    }


    private TopicRouteData buildTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueData> queueDataLit = new ArrayList<QueueData>();
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();

        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setQueueDatas(queueDataLit);

        QueueData qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-10");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-11");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-12");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-13");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        // ////////////////////////////////////////
        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-10");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.18:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-11");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.11:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-12");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.12:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-13");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.14:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-14");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.14:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-15");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.15:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-16");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.16:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-17");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.17:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-18");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.18:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-19");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.19:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        return topicRouteData;
    }


    private TopicRouteData buildTopicRouteData2() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueData> queueDataLit = new ArrayList<QueueData>();
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();

        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setQueueDatas(queueDataLit);

        QueueData qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-10");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-11");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-12");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        qd = new QueueData();
        qd.setBrokerName("zpullmsg-60-13");
        qd.setReadQueueNums(4);
        qd.setWriteQueueNums(4);
        qd.setPerm(6);
        queueDataLit.add(qd);

        // ////////////////////////////////////////
        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-10");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.18:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-11");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.11:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-12");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.12:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-13");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.14:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-14");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.14:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-15");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.15:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-16");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.16:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-17");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.17:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }

        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-19");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.19:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }
        {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("zpullmsg-60-18");
            HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
            brokerAddrs.put(0L, "10.228.188.18:10911");
            bd.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(bd);
        }
        return topicRouteData;
    }
}
