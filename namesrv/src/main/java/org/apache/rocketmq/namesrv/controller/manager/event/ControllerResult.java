package org.apache.rocketmq.namesrv.controller.manager.event;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 17:01
 */
public class ControllerResult<T> {
    private List<EventMessage> events;
    private T response;

    public ControllerResult() {
        this.events = new ArrayList<>();
    }

    public ControllerResult(T response) {
        this.events = new ArrayList<>();
        this.response = response;
    }

    public ControllerResult(List<EventMessage> events, T response) {
        this.events = events;
        this.response = response;
    }

    public List<EventMessage> getEvents() {
        return events;
    }

    public T getResponse() {
        return response;
    }

    public static <T> ControllerResult<T> of(List<EventMessage> events, T response) {
        return new ControllerResult<>(events, response);
    }

    public void addEvent(EventMessage event) {
        this.events.add(event);
    }
}
