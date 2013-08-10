package com.alibaba.rocketmq.broker.digestlog;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.GetMessageResult;


public class PullmsgLiveMoniter {

    private static final Logger log = LoggerFactory.getLogger("PullmsgLiveMoniter");


    public static void printProcessRequestLive(Channel channel, RemotingCommand request,
            GetMessageResult getMessageResult) {
        if (log.isInfoEnabled()) {
            log.info("receive [{}] PullMessage request command[{}] and return result [{}] ",
                RemotingHelper.parseChannelRemoteName(channel), request, getMessageResult);
        }
    }
}
