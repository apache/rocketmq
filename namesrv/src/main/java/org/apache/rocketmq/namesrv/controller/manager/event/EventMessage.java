package org.apache.rocketmq.namesrv.controller.manager.event;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 10:24
 */
public interface EventMessage {

    /**
     * Returns the event type of this message
     */
    EventType getEventType();
}
