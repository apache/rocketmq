package com.alibaba.rocketmq.broker.digestlog;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.PutMessageResult;


public class SendmsgLiveMoniter {
    private static final Logger log = LoggerFactory.getLogger("SendmsgLiveMoniter");


    public static void printProcessSendmsgRequestLive(Channel channel, RemotingCommand request,
            PutMessageResult putMessageResult) {
        if (log.isInfoEnabled()) {
            log.info("receive [{}] SendMessage request command[{}] and return result [{}] ",
                RemotingHelper.parseChannelRemoteName(channel), request, putMessageResult);
        }
    }
}
