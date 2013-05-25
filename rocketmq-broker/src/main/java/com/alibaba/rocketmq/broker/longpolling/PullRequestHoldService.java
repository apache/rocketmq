/**
 * $Id: PullRequestHoldService.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 拉消息请求管理，如果拉不到消息，则在这里Hold住，等待消息到来
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(MixAll.BrokerLoggerName);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    private ConcurrentHashMap<String/* topic@queueid */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    private final BrokerController brokerController;


    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }


    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }


    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (kArray != null && 2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                this.notifyMessageArriving(topic, queueId, offset);
            }
        }
    }


    public void notifyMessageArriving(final String topic, final int queueId, final long offset) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    // 查看是否offset OK
                    if (offset >= request.getPullFromThisOffset()) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                request.getClientChannel(), request.getRequestCommand());
                        }
                        catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    }
                    // 尝试取最新Offset
                    else {
                        final long newestOffset =
                                this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                        if (newestOffset >= request.getPullFromThisOffset()) {
                            try {
                                this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                    request.getClientChannel(), request.getRequestCommand());
                            }
                            catch (RemotingCommandException e) {
                                log.error("", e);
                            }
                            continue;
                        }
                    }

                    // 查看是否超时
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                request.getClientChannel(), request.getRequestCommand());
                        }
                        catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    }

                    // 当前不满足要求，重新放回Hold列表中
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }


    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStoped()) {
            try {
                this.waitForRunning(1000);
                this.checkHoldRequest();
            }
            catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }
}
