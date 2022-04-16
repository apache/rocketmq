package org.apache.rocketmq.namesrv.controller.event;

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
    public EventType eventType() {
        return EventType.READ_EVENT;
    }
}
