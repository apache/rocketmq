package com.alibaba.rocketmq.namesrv.topic;

import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Map;

import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.namesrv.common.MergeResult;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author lansheng.zj@taobao.com
 */
public interface TopicRuntimeDataManager {

    //
    // public void getBrokerList();
    // public void registerOrderTopic();
    // public void unRegisterOrderTopic();
    // public void getOrderTopicList();

    /**
     * 获取topic的路由信息
     * 
     * @param topic
     */
    public RemotingCommand getRouteInfoByTopic(String topic);


    /**
     * 获取TopicRuntimeData的一个snapshot
     * 
     * @return
     */
    public RemotingCommand getTopicRuntimeData();


    /**
     * 合并TopicRuntimeData数据
     * 
     * @param newTopicData
     * @return 合并的结果 {@link MergeResult}
     */
    public int merge(TopicRuntimeData newTopicData);


    /**
     * 注册broker地址到本地，并且扩散到其余namesrv结点
     * 
     * @param address
     *            broker地址
     * @return
     */
    public RemotingCommand registerBroker(String address);


    /**
     * 注册broker地址到本地
     * 
     * @param address
     * @return
     */
    public RemotingCommand registerBrokerSingle(String address);


    /**
     * 注销本地已注册的broker，并且扩散到其余的namesrv结点
     * 
     * @param brokerName
     *            已注册的brokername
     * @return
     */
    public RemotingCommand unRegisterBroker(String brokerName);


    /**
     * 注销本地已注册的broker
     * 
     * @param brokerName
     *            已注册的brokername
     * @return
     */
    public RemotingCommand unRegisterBrokerSingle(String brokerName);


    /**
     * 注册顺序消息的配置到本地，并且扩散到其余namesrv结点
     * 
     * @param topic
     * @param orderConf
     * @return
     */
    public RemotingCommand registerOrderTopic(String topic, String orderConf);


    /**
     * 注册顺序消息的配置到本地
     * 
     * @param topic
     * @param orderConf
     * @return
     */
    public RemotingCommand registerOrderTopicSingle(String topic, String orderConf);


    /**
     * 合并QueueData数据
     * 
     * @param queueDataMap
     * @return
     */
    public boolean mergeQueueData(Map<String, QueueData> queueDataMap);


    /**
     * 合并BrokerData数据
     * 
     * @param brokerName
     * @param brokerId
     * @param address
     * @return
     */
    public boolean mergeBrokerData(String brokerName, long brokerId, String address);


    /**
     * 注销地址为addr的broker相关数据
     * 
     * @param addr
     * @return
     */
    public RemotingCommand unRegisterBrokerSingleByAddr(String addr);


    /**
     * 注销地址为addr的broker相关数据
     * 
     * @param addr
     * @return
     */
    public RemotingCommand unRegisterBrokerByAddr(String addr);


    /**
     * 关闭，资源清理
     */
    public void shutdown();


    /**
     * 获取broker地址列表
     * 
     * @return
     */
    public ArrayList<String> getBrokerList();


    /**
     * 初始化
     * 
     * @return
     */
    public boolean init();


    /**
     * add property change listener
     * 
     * @param propertyName
     * @param listener
     */
    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener);


    /**
     * remove property change listener
     * 
     * @param listener
     */
    public void removePropertyChangeListener(final PropertyChangeListener listener);


    /**
     * fire property change
     * 
     * @param propertyName
     * @param oldValue
     * @param newValue
     */
    public void firePropertyChange(String propertyName, Object oldValue, Object newValue);

}
