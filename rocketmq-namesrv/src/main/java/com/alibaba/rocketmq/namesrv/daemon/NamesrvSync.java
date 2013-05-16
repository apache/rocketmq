package com.alibaba.rocketmq.namesrv.daemon;

import static com.alibaba.rocketmq.common.protocol.MetaProtos.MQRequestCode.SYNC_NAMESRV_RUNTIME_CONF_VALUE;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.MERGE_INVALID;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.MERGE_SUCCESS;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.NOT_MERGE;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.SYS_ERROR;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.sync.Exec;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskGroupExecutor;
import com.alibaba.rocketmq.namesrv.topic.TopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * namesrv集群各节点间数据同步
 * 
 * @author lansheng.zj@taobao.com
 */
public class NamesrvSync extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(MixAll.NamesrvLoggerName);
    private TaskGroupExecutor<Object, Object> syncTaskGroupExecutor;
    private TopicRuntimeDataManager topicRuntimeDataManager;
    private NamesrvConfig namesrvConf;
    private TaskGroup<Object, Object> syncTaskGroup;


    public NamesrvSync(NamesrvConfig namesrvConf, TopicRuntimeDataManager topicRuntimeDataManager) {
        this.namesrvConf = namesrvConf;
        this.topicRuntimeDataManager = topicRuntimeDataManager;
        syncTaskGroup = createTaskGroup();
        syncTaskGroupExecutor = new TaskGroupExecutor<Object, Object>(new ThreadFactory() {

            private AtomicInteger index = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "sync-taskgroup-executor-" + index.getAndIncrement());
            }
        });
    }


    private TaskGroup<Object, Object> createTaskGroup() {
        TaskGroup<Object, Object> taskGroup = new TaskGroup<Object, Object>();
        String namesrv = namesrvConf.getNamesrvAddr();
        if (null != namesrv && !"".equals(namesrv)) {
            String[] addresses = namesrv.split(";");
            for (String address : addresses) {
                // 排除本机，本机不注册任务
                if (!MixAll.isLocalAddr(address)) {
                    taskGroup.addTask(address, createTask(address));
                }
            }
        }

        return taskGroup;
    }


    public void init() {
        namesrvConf.addPropertyChangeListener("namesrvAddr", new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                // copyOnWirte
                syncTaskGroup = createTaskGroup();
            }
        });
    }


    private Exec<Object, Object> createTask(final String address) {

        return new Exec<Object, Object>() {

            @Override
            public void beforeExec() {
                // TODO log

            }


            @Override
            public Object doExec(Object object) throws Exception {

                // 主动同步各节点间数据，主动拉取方式，只同步namesrv上持久化数据

                RemotingCommand request = RemotingCommand.createRequestCommand(SYNC_NAMESRV_RUNTIME_CONF_VALUE, null);
                RemotingCommand response =
                        RemotingHelper.invokeSync(address, request, namesrvConf.getSyncTimeout());

                if (SUCCESS_VALUE == response.getCode()) {
                    byte[] data = response.getBody();
                    TopicRuntimeData topicRuntimeData = TopicRuntimeData.decode(data);

                    // 合并数据,相同key数据不同则删除数据（还是有问题，没法删除掉，不做删除）
                    int code = topicRuntimeDataManager.merge(topicRuntimeData);
                    switch (code) {
                    case NOT_MERGE:
                        // TODO 统计数据
                        break;
                    case MERGE_SUCCESS:
                        // TODO 统计数据
                        break;
                    case MERGE_INVALID:
                        // TODO 报警
                        break;
                    case SYS_ERROR:
                        // TODO 报警
                        break;
                    default:
                    }
                }
                else {
                    // TODO
                }

                return null;
            }

        };
    }


    public FutureGroup<Object> groupCommit() {
        return syncTaskGroupExecutor.groupCommit(syncTaskGroup, null);
    }


    @Override
    public void run() {

        try {
            // initial delay
            Thread.sleep(2000L);
        }
        catch (InterruptedException e) {
            log.error("namesrv sync initial delay interrupted.", e);
        }

        while (!isStoped()) {
            try {
                FutureGroup<Object> futureGroup = groupCommit();
                long timeout = namesrvConf.getSyncTimeout();
                Result result = futureGroup.await(timeout);
                if (!result.isSuccess()) {
                    // 超时未返回报警
                    log.error("namesrv sync timeout(" + timeout + "). detail:" + result.getDetail());
                }

                this.waitForRunning(namesrvConf.getSyncInterval());
            }
            catch (Exception e) {
                // exception handler
                log.error("namesrv sync exception.", e);
            }
        }
    }


    @Override
    public String getServiceName() {
        return "namesrv-sync";
    }


    public void shutdown() {
        super.shutdown();
        syncTaskGroupExecutor.shutdown();
    }


    @Override
    public void stop() {
        super.stop();
        syncTaskGroupExecutor.shutdown();
    }


    @Override
    public void stop(boolean interrupt) {
        super.stop(interrupt);
        syncTaskGroupExecutor.shutdown();
    }

}
