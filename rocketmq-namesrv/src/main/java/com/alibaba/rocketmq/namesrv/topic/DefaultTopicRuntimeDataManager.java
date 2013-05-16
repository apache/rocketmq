package com.alibaba.rocketmq.namesrv.topic;

import static com.alibaba.rocketmq.common.MixAll.Localhost;
import static com.alibaba.rocketmq.common.namesrv.TopicRuntimeData.ORDER_PREFIX;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.REGISTER_BROKER_FAIL_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.REGISTER_BROKER_TIMEOUT_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.REGISTER_ORDER_TOPIC_FAIL_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.REGISTER_ORDER_TOPIC_TIMEOUT_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.TOPIC_NOT_EXIST_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.UNREGISTER_BROKER_FAIL_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode.UNREGISTER_BROKER_TIMEOUT_VALUE;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.MERGE_INVALID;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.NOT_MERGE;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.SYS_ERROR;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.REG_BROKER;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.REG_TOPIC;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.UNREG_BROKER;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;

import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.route.ObjectConverter;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.namesrv.common.MergeResultHolder;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.processor.ChangeSpreadProcessor;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author lansheng.zj@taobao.com
 */
public class DefaultTopicRuntimeDataManager implements TopicRuntimeDataManager {

    private static final Logger log = LoggerFactory.getLogger(MixAll.NamesrvLoggerName);

    private NamesrvConfig namesrvConfig;
    private TopicRuntimeData topicData;
    private ChangeSpreadProcessor changeSpread;
    private ReentrantReadWriteLock lock;
    private ReadLock readLock;
    private WriteLock writeLock;


    // TODO version or md5 checksum

    public DefaultTopicRuntimeDataManager(NamesrvConfig config) {
        namesrvConfig = config;
        topicData = new TopicRuntimeData();
        changeSpread = new ChangeSpreadProcessor(namesrvConfig);

        lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }


    public boolean init() {

        // 加载初始化数据
        if (!loadBrokerList() || !loadOrderConf())
            return false;

        changeSpread.init();
        return true;
    }


    private boolean loadBrokerList() {
        try {
            String content = MixAll.file2String(namesrvConfig.getBrokerAddrConfPath());
            if (null == content)
                return true;

            String[] addresses = content.split(";");
            List<String> array = new ArrayList<String>();
            for (String addr : addresses) {
                array.add(addr);
            }
            topicData.setBrokerList(array);
            topicData.firePropertyChange("brokerList", null, array);

            if (log.isInfoEnabled())
                log.info("load broker list:" + content);
            return true;
        }
        catch (Exception e) {
            // TODO log

        }
        return false;
    }


    private boolean loadOrderConf() {
        try {
            File file = new File(namesrvConfig.getOrderConfPath());
            if (!file.exists())
                return true;

            FileInputStream fis = new FileInputStream(file);
            Properties propers = new Properties();
            propers.load(fis);
            Map<String, String> topicOrderConfs = new HashMap<String, String>();
            for (Entry<Object, Object> entry : propers.entrySet()) {
                String key = (String) entry.getKey();
                if (!topicOrderConfs.containsKey(key))
                    topicOrderConfs.put(key, (String) entry.getValue());
            }
            topicData.setTopicOrderConfs(topicOrderConfs);
            fis.close();
            if (log.isInfoEnabled())
                log.info("load topic order conf:\n" + propers);
            return true;
        }
        catch (Exception e) {
            // TODO log

            e.printStackTrace();
        }
        return false;
    }


    public int merge(TopicRuntimeData newTopicData) {
        boolean equal = false;

        readLock.lock();
        try {
            equal = topicData.equals(newTopicData);
        }
        catch (Exception e) {
            // TODO
        }
        finally {
            readLock.unlock();
        }

        if (!equal) {
            writeLock.lock();
            try {
                return doMerge(newTopicData);
            }
            catch (Throwable e) {
                // TODO log

                e.printStackTrace();
                return SYS_ERROR;
            }
            finally {
                writeLock.unlock();
            }
        }

        return NOT_MERGE;
    }


    private int doMerge(TopicRuntimeData newTopicData) {
        if (log.isInfoEnabled())
            log.info("namesrv data merge start ...");

        MergeResultHolder ret = new MergeResultHolder();

        // if(!ObjectConverter.equals(topicData.getTopicBrokers(),
        // newTopicData.getTopicBrokers())) {
        // topicData.setTopicBrokers(mergeMap(topicData.getTopicBrokers(),
        // newTopicData.getTopicBrokers(), ret));
        // }

        if (!ObjectConverter.equals(topicData.getTopicOrderConfs(), newTopicData.getTopicOrderConfs())) {
            topicData.setTopicOrderConfs(mergeMap(topicData.getTopicOrderConfs(),
                newTopicData.getTopicOrderConfs(), ret));
            storeOrderTopic();
        }

        // if(!ObjectConverter.equals(topicData.getBrokers(),
        // newTopicData.getBrokers())) {
        // topicData.setBrokers(mergeMap(topicData.getBrokers(),
        // newTopicData.getBrokers(), ret));
        // }

        if (!ObjectConverter.equals(topicData.getBrokerList(), newTopicData.getBrokerList())) {
            topicData.setBrokerList(mergeList(topicData.getBrokerList(), newTopicData.getBrokerList()));
            storeBrokerList();
        }

        if (log.isInfoEnabled())
            log.info("namesrv data merge end ...");

        return ret.getResult();
    }


    private <K, V> Map<K, V> mergeMap(Map<K, V> oldMap, Map<K, V> newMap, MergeResultHolder holder) {
        Map<K, V> r = new HashMap<K, V>();
        for (Entry<K, V> entry : oldMap.entrySet()) {
            K oldKey = entry.getKey();
            V oldV = entry.getValue();
            if (newMap.containsKey(oldKey)) {
                if (null == oldV || oldV.equals(newMap.get(oldKey))) {
                    r.put(oldKey, null == oldV ? newMap.get(oldKey) : oldV);
                }
                else {
                    holder.setResult(MERGE_INVALID);

                    // key相同但值不同,数据严重不一致报警
                    log.error("fetal error merge inconsistent data, key:" + oldKey + ",local value:" + oldV
                            + ",remote value:" + newMap.get(oldKey));

                }
            }
            else {
                r.put(oldKey, oldV);
            }
        }

        for (Entry<K, V> entry : newMap.entrySet()) {
            K newKey = entry.getKey();
            if (!oldMap.containsKey(newKey)) {
                r.put(newKey, entry.getValue());
            }
        }

        return r;
    }


    private <E> List<E> mergeList(List<E> oldList, List<E> newList) {
        List<E> r = new ArrayList<E>();
        r.addAll(oldList);
        for (E e : newList) {
            if (!r.contains(e))
                r.add(e);
        }
        return r;
    }


    private Result spreadSuccess(FutureGroup<Result> future) {
        Result result = Result.SUCCESS;
        try {
            for (Entry<String, Future<Result>> entry : future.getResultMap().entrySet()) {
                if (!entry.getValue().get().isSuccess()) {
                    result = new Result(false, future.detailInfo());
                    break;
                }
            }
        }
        catch (Exception e) {
            // exception handle
            log.error(Localhost + " spreadSuccess exception", e);
            result = new Result(false, Localhost + " spreadSuccess exception" + e.getMessage());
        }

        return result;
    }


    @Override
    public RemotingCommand getRouteInfoByTopic(String topic) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        TopicRouteData snapshot = doGetRouteInfoByTopic(topic);
        if (null != snapshot) {
            response.setBody(snapshot.encode());
            response.setCode(SUCCESS_VALUE);
        }
        else {
            response.setCode(TOPIC_NOT_EXIST_VALUE);
            response.setRemark("No topic config in this Name Server.");
        }

        return response;
    }


    private TopicRouteData doGetRouteInfoByTopic(String topic) {
        readLock.lock();
        try {
            List<QueueData> queueDatas = topicData.getTopicBrokers().get(topic);
            if (null != queueDatas) {
                TopicRouteData topicRouteData = new TopicRouteData();
                topicRouteData.setQueueDatas(queueDatas);
                List<BrokerData> brokerDatas = new ArrayList<BrokerData>();
                for (QueueData queueData : queueDatas) {
                    if (topicData.getBrokers().containsKey(queueData.getBrokerName())) {
                        brokerDatas.add(topicData.getBrokers().get(queueData.getBrokerName()));
                    }
                }
                topicRouteData.setBrokerDatas(brokerDatas);
                topicRouteData.setOrderTopicConf(topicData.getOrderConfByTopic(topic));

                return topicRouteData;
            }
        }
        finally {
            readLock.unlock();
        }
        return null;
    }


    @Override
    public RemotingCommand getTopicRuntimeData() {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        byte[] data = serializeTopicRuntimeData();
        response.setBody(data);
        response.setCode(SUCCESS_VALUE);
        return response;
    }


    public byte[] serializeTopicRuntimeData() {
        readLock.lock();
        try {
            return topicData.encodeSpecific();
        }
        finally {
            readLock.unlock();
        }
    }


    @Override
    public RemotingCommand registerBrokerSingle(String address) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        doRegisterBroker(address);
        response.setCode(SUCCESS_VALUE);

        if (log.isInfoEnabled())
            log.info("register broker single, address:" + address);

        return response;
    }


    @Override
    public RemotingCommand registerBroker(String address) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // register to local
        doRegisterBroker(address);

        // spread to other namesrv
        FutureGroup<Result> future = changeSpread.spreadChange(REG_BROKER, new String[] { address });

        // set return value
        Result result = future.await(namesrvConfig.getGroupWaitTimeout());
        if (result.isSuccess()) {
            result = spreadSuccess(future);
            if (result.isSuccess()) {
                response.setCode(SUCCESS_VALUE);
            }
            else {
                response.setCode(REGISTER_BROKER_FAIL_VALUE);
                response.setRemark(result.getDetail());
            }
        }
        else {
            response.setCode(REGISTER_BROKER_TIMEOUT_VALUE);
            response.setRemark(result.getDetail());
        }

        if (log.isInfoEnabled())
            log.info("register broker and spread to others, address:" + address + ",result:" + result.isSuccess()
                    + ",code:" + response.getCode());

        return response;
    }


    public void doRegisterBroker(String address) {
        readLock.lock();
        try {
            List<String> brokerList = topicData.getBrokerList();
            if (!brokerList.contains(address)) {
                brokerList.add(address);
                storeBrokerList();
                topicData.firePropertyChange("brokerList", null, null);
            }
        }
        finally {
            readLock.unlock();
        }
    }


    private void storeOrderTopic() {
        String content = topicData.encodeOrderTopicAsString();
        String fileName = namesrvConfig.getOrderConfPath();
        boolean success = MixAll.string2File(content, fileName);

        log.info("store order topic in local " + success);
    }


    private void storeBrokerList() {
        String content = topicData.encodeBrokerListAsString();
        String fileName = namesrvConfig.getBrokerAddrConfPath();
        boolean success = MixAll.string2File(content, fileName);

        log.info("store broker list in local " + success);
    }


    /**
     * 
     * @param topic
     * @param orderConf
     */
    public boolean doRegisterOrderTopic(String topic, String orderConf) {
        boolean success = true;
        readLock.lock();
        try {
            Map<String, String> orderTopicConf = topicData.getTopicOrderConfs();
            String key = ORDER_PREFIX + topic;
            if (orderTopicConf.containsKey(key)) {
                String oldOrderConf = orderTopicConf.get(key);
                if (oldOrderConf != null && !oldOrderConf.equals(orderConf)) {
                    success = false;

                    // key相同但值不同,数据严重不一致报警
                    log.error("fetal error register order topic, key:" + key + ",local value:" + oldOrderConf
                            + ",remote value:" + orderConf);
                }
            }
            else {
                orderTopicConf.put(key, orderConf);
                if (log.isInfoEnabled())
                    log.info("start register order topic, topic:" + key + ",order conf:" + orderConf);
                // store in local
                storeOrderTopic();
            }
        }
        finally {
            readLock.unlock();
        }

        return success;
    }


    @Override
    public RemotingCommand registerOrderTopic(String topic, String orderConf) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        if (!doRegisterOrderTopic(topic, orderConf)) {
            response.setCode(REGISTER_ORDER_TOPIC_FAIL_VALUE);
            return response;
        }

        FutureGroup<Result> future = changeSpread.spreadChange(REG_TOPIC, new String[] { topic, orderConf });
        Result result = future.await(namesrvConfig.getGroupWaitTimeout());
        if (result.isSuccess()) {
            result = spreadSuccess(future);
            if (result.isSuccess()) {
                response.setCode(SUCCESS_VALUE);
            }
            else {
                response.setCode(REGISTER_ORDER_TOPIC_FAIL_VALUE);
                response.setRemark(result.getDetail());
            }
        }
        else {
            response.setCode(REGISTER_ORDER_TOPIC_TIMEOUT_VALUE);
            response.setRemark(result.getDetail());
        }

        return response;
    }


    @Override
    public RemotingCommand registerOrderTopicSingle(String topic, String orderConf) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(REGISTER_ORDER_TOPIC_FAIL_VALUE);
        if (doRegisterOrderTopic(topic, orderConf)) {
            response.setCode(SUCCESS_VALUE);
        }

        return response;
    }


    @Override
    public RemotingCommand unRegisterBroker(String brokerName) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (null == brokerName || "".equals(brokerName)) {
            response.setCode(MQResponseCode.UNREGISTER_BROKER_FAIL_VALUE);
            response.setRemark("empty brokerName");
        }

        if (doUnRegisterBroker(brokerName)) {
            response.setCode(MQResponseCode.UNREGISTER_BROKER_FAIL_VALUE);
            // FIXME detail info
            response.setRemark("");
        }

        // spread to other namesrv
        FutureGroup<Result> future = changeSpread.spreadChange(UNREG_BROKER, new String[] { brokerName });
        // set return value
        Result result = future.await(namesrvConfig.getGroupWaitTimeout());
        if (result.isSuccess()) {
            result = spreadSuccess(future);
            if (result.isSuccess()) {
                response.setCode(SUCCESS_VALUE);
            }
            else {
                response.setCode(UNREGISTER_BROKER_FAIL_VALUE);
                response.setRemark(result.getDetail());
            }
        }
        else {
            response.setCode(UNREGISTER_BROKER_TIMEOUT_VALUE);
            response.setRemark(result.getDetail());
        }

        if (log.isInfoEnabled())
            log.info("register broker and spread to others, brokername:" + brokerName + ",result:"
                    + result.isSuccess() + ",code:" + response.getCode());

        return response;
    }


    @Override
    public RemotingCommand unRegisterBrokerSingle(String brokerName) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(UNREGISTER_BROKER_FAIL_VALUE);
        if (doUnRegisterBroker(brokerName)) {
            response.setCode(SUCCESS_VALUE);
        }

        return response;
    }


    @Override
    public boolean mergeQueueData(Map<String, QueueData> queueDataMap) {
        writeLock.lock();
        try {
            Map<String, List<QueueData>> queueMap = topicData.getTopicBrokers();
            for (Entry<String, QueueData> entry : queueDataMap.entrySet()) {
                String newKey = entry.getKey();
                QueueData newQueueData = entry.getValue();
                if (queueMap.containsKey(newKey)) {
                    boolean append = true;
                    List<QueueData> oldQueues = queueMap.get(newKey);
                    for (QueueData old : oldQueues) {
                        if (old.getBrokerName().equals(newQueueData.getBrokerName())) {
                            oldQueues.remove(old);
                            oldQueues.add(newQueueData);
                            append = false;
                            break;
                        }
                    }
                    if (append)
                        oldQueues.add(newQueueData);
                }
                else {
                    List<QueueData> queues = new ArrayList<QueueData>();
                    queues.add(newQueueData);
                    queueMap.put(newKey, queues);
                }
            }
        }
        finally {
            writeLock.unlock();
        }

        return true;
    }


    @Override
    public boolean mergeBrokerData(String brokerName, long brokerId, String address) {
        writeLock.lock();
        try {
            Map<String, BrokerData> brokerMap = topicData.getBrokers();
            if (brokerMap.containsKey(brokerName)) {
                BrokerData brokerData = brokerMap.get(brokerName);
                brokerData.getBrokerAddrs().put(brokerId, address);
            }
            else {
                BrokerData brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                HashMap<Long, String> addrMap = new HashMap<Long, String>();
                addrMap.put(brokerId, address);
                brokerData.setBrokerAddrs(addrMap);
                brokerMap.put(brokerName, brokerData);
            }
        }
        finally {
            writeLock.unlock();
        }

        return true;
    }


    public boolean doUnRegisterBroker(String brokerName) {
        writeLock.lock();
        try {
            // remove broker
            topicData.getBrokers().remove(brokerName);

            // remove topic info
            Iterator<Entry<String, List<QueueData>>> iterator = topicData.getTopicBrokers().entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, List<QueueData>> entry = iterator.next();
                List<QueueData> listQueue = entry.getValue();
                Iterator<QueueData> iterQueueData = listQueue.iterator();
                while (iterQueueData.hasNext()) {
                    QueueData queueData = iterQueueData.next();
                    if (queueData.getBrokerName().equals(brokerName)) {
                        iterQueueData.remove();
                    }
                }
                if (listQueue.size() <= 0)
                    iterator.remove();
            }

            return true;
        }
        finally {
            writeLock.unlock();
        }
    }


    @Override
    public boolean unRegisterBrokerByAddr(String addr) {
        if (null == addr || "".equals(addr))
            return false;

        String brokerName = findBrokerNameByAddr(addr);
        return doUnRegisterBroker(brokerName);
    }


    private String findBrokerNameByAddr(String addr) {
        readLock.lock();
        try {
            for (Entry<String, BrokerData> entry : topicData.getBrokers().entrySet()) {
                BrokerData brokerData = entry.getValue();
                for (Entry<Long, String> brokerEntry : brokerData.getBrokerAddrs().entrySet()) {
                    if (brokerEntry.getValue().equals(addr))
                        return brokerData.getBrokerName();
                }
            }
            return null;
        }
        finally {
            readLock.unlock();
        }
    }


    @Override
    public ArrayList<String> getBrokerList() {
        return (ArrayList<String>) ((ArrayList<String>) topicData.getBrokerList()).clone();
    }


    @Override
    public void shutdown() {
        changeSpread.shutdown();
    }


    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener) {
        topicData.addPropertyChangeListener(propertyName, listener);
    }


    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        topicData.removePropertyChangeListener(listener);
    }


    public void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
        topicData.firePropertyChange(propertyName, oldValue, newValue);
    }

}
