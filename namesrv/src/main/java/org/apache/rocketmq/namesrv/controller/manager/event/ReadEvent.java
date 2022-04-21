package org.apache.rocketmq.namesrv.controller.manager.event;

/**
 * Read event.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:08
 */
public class ReadEvent implements EventMessage {

    public ReadEvent() {
    }

    @Override
    public EventType getEventType() {
        return EventType.READ_EVENT;
    }
}
