package com.alibaba.rocketmq.tools.monitor;

import com.alibaba.rocketmq.common.protocol.topic.OffsetMovedEvent;


public class DeleteMsgsEvent {
    private OffsetMovedEvent offsetMovedEvent;
    private long eventTimestamp;
}
