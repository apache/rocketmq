package com.alibaba.rocketmq.namesrv.processor;

import static com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode.REGISTER_BROKER_SINGLE_VALUE;
import static com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode.REGISTER_ORDER_TOPIC_SINGLE_VALUE;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.REG_BROKER;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.REG_TOPIC;
import static com.alibaba.rocketmq.namesrv.sync.TaskType.UNREG_BROKER;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterOrderTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.sync.Exec;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroupExecutor;
import com.alibaba.rocketmq.namesrv.sync.TaskType;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author lansheng.zj@taobao.com
 */
public class ChangeSpreadProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    private Map<Integer, TaskGroup<Result, String[]>> taskGroupMap;
    private TaskGroupExecutor<Result, String[]> clientTaskGroupExecutor;
    private NamesrvConfig namesrvConf;


    public ChangeSpreadProcessor(NamesrvConfig namesrvConf) {
        this.namesrvConf = namesrvConf;
        taskGroupMap = createTaskMap();
        clientTaskGroupExecutor = new TaskGroupExecutor<Result, String[]>(new ThreadFactory() {

            private AtomicInteger index = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "client-taskgroup-executor-" + index.getAndIncrement());
            }
        });
    }


    public void init() {
        namesrvConf.addPropertyChangeListener("namesrvAddr", new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                // copyOnWirte
                taskGroupMap = createTaskMap();
            }
        });
    }


    public Map<Integer, TaskGroup<Result, String[]>> createTaskMap() {
        Map<Integer, TaskGroup<Result, String[]>> map = new HashMap<Integer, TaskGroup<Result, String[]>>();
        map.put(REG_BROKER, createTaskGroup(REG_BROKER));
        map.put(REG_TOPIC, createTaskGroup(REG_TOPIC));
        map.put(UNREG_BROKER, createTaskGroup(UNREG_BROKER));
        return map;
    }


    public FutureGroup<Result> spreadChange(int type, String[] values) {
        TaskGroup<Result, String[]> taskGroup = null;
        switch (type) {
        case REG_BROKER:
            taskGroup = taskGroupMap.get(REG_BROKER);
            break;
        case REG_TOPIC:
            taskGroup = taskGroupMap.get(REG_TOPIC);
            break;
        case UNREG_BROKER:
            taskGroup = taskGroupMap.get(UNREG_BROKER);
            break;
        default:
            // TODO
        }

        return clientTaskGroupExecutor.groupCommit(taskGroup, values);
    }


    /**
     * 创建任务组
     * 
     * @param type
     *            任务类型 {@link TaskType}
     * @return
     */
    public TaskGroup<Result, String[]> createTaskGroup(int type) {

        // 排除本机，本机不注册任务

        TaskGroup<Result, String[]> taskGroup = new TaskGroup<Result, String[]>();
        String namesrv = namesrvConf.getNamesrvAddr();
        if (null != namesrv && !"".equals(namesrv)) {
            String[] addresses = namesrv.split(";");
            for (String address : addresses) {
                if (!MixAll.isLocalAddr(address)) {
                    switch (type) {
                    case REG_BROKER:
                        taskGroup.addTask(address, createRegBrokerTask(address));
                        break;
                    case REG_TOPIC:
                        taskGroup.addTask(address, createRegTopicTask(address));
                        break;
                    case UNREG_BROKER:
                        taskGroup.addTask(address, createUnRegBrokerTask(address));
                        break;
                    default:
                        // TODO
                    }
                }
            }
        }

        return taskGroup;
    }


    public Exec<Result, String[]> createRegBrokerTask(final String address) {
        return new Exec<Result, String[]>() {

            @Override
            public void beforeExec() {
                // TODO
            }


            @Override
            public Result doExec(String[] brokerAddr) throws Exception {
                RegisterBrokerRequestHeader regBrokerRequestHeader = new RegisterBrokerRequestHeader();
                regBrokerRequestHeader.setBrokerAddr(brokerAddr[0]);
                RemotingCommand request =
                        RemotingCommand.createRequestCommand(REGISTER_BROKER_SINGLE_VALUE, regBrokerRequestHeader);
                RemotingCommand response =
                        RemotingHelper.invokeSync(address, request, namesrvConf.getSyncTimeout());
                if (SUCCESS_VALUE == response.getCode()) {
                    // TODO

                    return Result.SUCCESS;
                }
                else {
                    // record failure
                    log.error("register broker single fail. code" + response.getCode() + ",remark:"
                            + response.getRemark());
                    return new Result(false, "code:" + response.getCode() + ",remark:" + response.getRemark());
                }
            }

        };
    }


    public Exec<Result, String[]> createRegTopicTask(final String address) {
        return new Exec<Result, String[]>() {

            @Override
            public void beforeExec() {
                // TODO

            }


            @Override
            public Result doExec(String[] ts) throws Exception {
                RegisterOrderTopicRequestHeader regOrderTopicRequestHeader = new RegisterOrderTopicRequestHeader();
                regOrderTopicRequestHeader.setTopic(ts[0]);
                regOrderTopicRequestHeader.setOrderTopicString(ts[1]);
                RemotingCommand request =
                        RemotingCommand.createRequestCommand(REGISTER_ORDER_TOPIC_SINGLE_VALUE,
                            regOrderTopicRequestHeader);

                RemotingCommand response =
                        RemotingHelper.invokeSync(address, request, namesrvConf.getSyncTimeout());
                if (SUCCESS_VALUE == response.getCode()) {
                    // TODO

                    return Result.SUCCESS;
                }
                else {
                    // record failure
                    log.error("register order topic single fail. code" + response.getCode() + ",remark:"
                            + response.getRemark());
                    return new Result(false, "code:" + response.getCode() + ",remark:" + response.getRemark());
                }
            }

        };
    }


    public Exec<Result, String[]> createUnRegBrokerTask(final String address) {
        return new Exec<Result, String[]>() {

            @Override
            public void beforeExec() {
                // TODO
            }


            @Override
            public Result doExec(String[] brokerName) throws Exception {
                UnRegisterBrokerRequestHeader unRegisterBrokerRequestHeader = new UnRegisterBrokerRequestHeader();
                unRegisterBrokerRequestHeader.setBrokerName(brokerName[0]);
                RemotingCommand request =
                        RemotingCommand.createRequestCommand(MQRequestCode.UNREGISTER_BROKER_SINGLE_VALUE,
                            unRegisterBrokerRequestHeader);
                RemotingCommand response =
                        RemotingHelper.invokeSync(address, request, namesrvConf.getSyncTimeout());
                if (SUCCESS_VALUE == response.getCode()) {
                    // TODO

                    return Result.SUCCESS;
                }
                else {
                    // record failure
                    log.error("register broker single fail. code" + response.getCode() + ",remark:"
                            + response.getRemark());
                    return new Result(false, "code:" + response.getCode() + ",remark:" + response.getRemark());
                }
            }

        };
    }


    public void shutdown() {
        clientTaskGroupExecutor.shutdown();
    }
}
