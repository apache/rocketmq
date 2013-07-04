package com.alibaba.rocketmq.namesrv.daemon;

import static com.alibaba.rocketmq.common.MixAll.Localhost;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode.GET_ALL_TOPIC_CONFIG_VALUE;
import static com.alibaba.rocketmq.common.protocol.route.ObjectConverter.props2TopicConfigTable;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;
import io.netty.channel.Channel;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetTopicResponseHeader;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.sync.Exec;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroupExecutor;
import com.alibaba.rocketmq.namesrv.topic.TopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 定时从broker获取topic配置信息任务线程
 * 
 * @author lansheng.zj@taobao.com
 */
public class NamesrvClient extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    public static final int MAX_RETRIES = 3;
    private TaskGroupExecutor<Boolean, Object> reqBrokerGroupExecutor;
    private TaskGroup<Boolean, Object> reqTaskGroup;
    private TopicRuntimeDataManager topicRuntimeDataManager;
    private RemotingClient remotingClient;
    private NettyClientConfig nettyClientConfig;
    private NamesrvConfig namesrvConf;


    public NamesrvClient(NamesrvConfig nameConf, NettyClientConfig nettyConfig,
            TopicRuntimeDataManager runtimeDataManager) {
        namesrvConf = nameConf;
        topicRuntimeDataManager = runtimeDataManager;
        nettyClientConfig = nettyConfig;
        remotingClient = new NettyRemotingClient(nettyClientConfig, new ChannelEventListener() {

            @Override
            public void onChannelConnect(String remoteAddr, Channel channel) {
                // ignore
            }


            @Override
            public void onChannelClose(String remoteAddr, Channel channel) {
                // unregister broker topic info when channel close
                unRegisterBrokerTopic(remoteAddr);
            }


            @Override
            public void onChannelException(String remoteAddr, Channel channel) {
                // unregister broker topic info when channel exception
                unRegisterBrokerTopic(remoteAddr);
            }


            @Override
            public void onChannelIdle(String remoteAddr, Channel channel) {
            }
        });

        reqTaskGroup = createTaskGroup();
        reqBrokerGroupExecutor = new TaskGroupExecutor<Boolean, Object>(new ThreadFactory() {

            private AtomicInteger index = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "reqbroker-taskgroup-executor-" + index.getAndIncrement());
            }
        });

        topicRuntimeDataManager.addPropertyChangeListener("brokerList", new PropertyChangeListener() {

            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                reqTaskGroup = createTaskGroup();
            }

        });
    }


    private TaskGroup<Boolean, Object> createTaskGroup() {
        TaskGroup<Boolean, Object> taskGroup = new TaskGroup<Boolean, Object>();
        for (String address : topicRuntimeDataManager.getBrokerList()) {
            taskGroup.addTask(address, createTask(address));
        }

        return taskGroup;
    }


    private Exec<Boolean, Object> createTask(final String address) {
        return new Exec<Boolean, Object>() {

            @Override
            public void beforeExec() {
                // TODO
            }


            @Override
            public Boolean doExec(Object param) throws Exception {
                return NamesrvClient.this.requestBrokerTopicConf(address);
            }
        };
    }


    public boolean requestBrokerTopicConf(String address) throws Exception {
        boolean success = false;
        int reties = 0;
        if (null == address || "".equals(address)) {
            log.error("request broker topic error because addr is blank, check broker addr retrieve");
            return success;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(GET_ALL_TOPIC_CONFIG_VALUE, null);

        while (!success && reties++ <= MAX_RETRIES) {
            try {
                RemotingCommand response =
                        remotingClient.invokeSync(address, request, namesrvConf.getPullFormBrokerTimeout());
                if (SUCCESS_VALUE == response.getCode()) {
                    GetTopicResponseHeader responseHeader =
                            (GetTopicResponseHeader) response
                                .decodeCommandCustomHeader(GetTopicResponseHeader.class);
                    // String version = responseHeader.getVersion();
                    // TODO charset problem
                    byte[] data = response.getBody();
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                    Properties props = new Properties();
                    props.load(inputStream);
                    Map<String, TopicConfig> snapshot = props2TopicConfigTable(props, null);
                    Map<String, QueueData> queueDataMap =
                            topicConfigMap2QueueDataMap(snapshot, responseHeader.getBrokerName());
                    // 合并从broker上拉取的topic配置信息
                    topicRuntimeDataManager.mergeQueueData(queueDataMap);
                    // 合并brokerData信息
                    topicRuntimeDataManager.mergeBrokerData(responseHeader.getBrokerName(),
                        responseHeader.getBrokerId(), address);

                    success = true;
                }
                else {
                    // record failure
                    log.error("get all topic config from broker(" + address + ") fail, code:"
                            + response.getCode() + ", remark:" + response.getRemark());
                    success = false;
                }
            }
            catch (Exception e) {
                if (MAX_RETRIES == reties) {
                    log.error("request broker(" + address + ") topic fail finally");
                    break;
                }

                log.error("request broker(" + address + ") topic fail, retry " + reties, e);
                Thread.sleep(500);
            }
        }
        return success;
    }


    public boolean unRegisterBrokerTopic(String address) {
        if (null == address || "".equals(address))
            return false;

        RemotingCommand rc = topicRuntimeDataManager.unRegisterBrokerByAddr(address);
        if (rc.getCode() == SUCCESS_VALUE)
            return true;

        // log
        return false;
    }


    private Map<String, QueueData> topicConfigMap2QueueDataMap(Map<String, TopicConfig> map, String brokerName) {
        Map<String, QueueData> queueDataMap = new HashMap<String, QueueData>();
        for (Entry<String, TopicConfig> entry : map.entrySet()) {
            QueueData queueData = new QueueData();
            queueData.setBrokerName(brokerName);
            queueData.setPerm(entry.getValue().getPerm());
            queueData.setReadQueueNums(entry.getValue().getReadQueueNums());
            queueData.setWriteQueueNums(entry.getValue().getWriteQueueNums());
            queueDataMap.put(entry.getKey(), queueData);
        }

        return queueDataMap;
    }


    public FutureGroup<Boolean> groupCommit() {
        return reqBrokerGroupExecutor.groupCommit(reqTaskGroup, null);
    }


    @Override
    public void run() {

        try {
            // initial delay
            Thread.sleep(2000L);
        }
        catch (InterruptedException e) {
            // TODO
            log.error("namesrvclient initial delay interruped.", e);
        }

        while (!isStoped()) {
            try {
                waitForRunning(namesrvConf.getPullFormBrokerInterval());
                FutureGroup<Boolean> futureGroup = groupCommit();
                Result result = futureGroup.await(namesrvConf.getPullFormBrokerTimeout());
                if (result.isSuccess()) {
                    Result pullResult = pullSuccess(futureGroup);
                    if (pullResult.isSuccess()) {
                        // TODO all success

                    }
                    else {
                        // someone fail
                        log.error("merge broker topic conf not all success. detail:\n"
                                + pullResult.getDetail());
                    }
                }
                else {
                    // 超时
                    log.error("merge broker topic conf timeout. detail:\n" + result.getDetail());
                }
            }
            catch (Exception e) {
                // error handler
                log.error("NamesrvClient.run exception.", e);
            }
        }
    }


    public Result pullSuccess(FutureGroup<Boolean> future) {
        Result result = Result.SUCCESS;
        try {
            for (Entry<String, Future<Boolean>> entry : future.getResultMap().entrySet()) {
                if (!entry.getValue().get().booleanValue()) {
                    result = new Result(false, future.detailInfo());
                    break;
                }
            }
        }
        catch (Exception e) {
            log.error("NamesrvClient.pullSuccess exception", e);
            result = new Result(false, Localhost + " NamesrvClient.pullSuccess exception" + e.getMessage());
        }

        return result;
    }


    @Override
    public void start() {
        remotingClient.start();
        super.start();
    }


    @Override
    public void stop() {
        super.stop();
        remotingClient.shutdown();

    }


    @Override
    public void stop(boolean interrupt) {
        super.stop(interrupt);
        remotingClient.shutdown();
    }


    @Override
    public String getServiceName() {
        return "namesrv-polling-broker";
    }

}
