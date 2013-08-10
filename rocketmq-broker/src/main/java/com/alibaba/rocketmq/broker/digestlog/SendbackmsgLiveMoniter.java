package com.alibaba.rocketmq.broker.digestlog;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.PutMessageResult;


public class SendbackmsgLiveMoniter {
    private static final Logger log = LoggerFactory.getLogger("SendbackmsgLiveMoniter");


    public static void printProcessSendmsgRequestLive(Channel channel, RemotingCommand request,
            PutMessageResult putMessageResult, int delayLevel, int reconsumeTimes) {
        if (log.isInfoEnabled()) {
            log.info(
                "receive [{}] SendBackMessage request command[{}] , this msg ReconsumeTimes[{}] and DelayLevel [{}]. return result [{}] ",
                RemotingHelper.parseChannelRemoteName(channel), request, reconsumeTimes, delayLevel,
                putMessageResult);
        }
    }
}
