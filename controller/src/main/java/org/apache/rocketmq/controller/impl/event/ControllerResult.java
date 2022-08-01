/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.controller.impl.event;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.protocol.ResponseCode;

public class ControllerResult<T> {
    private final List<EventMessage> events;
    private final T response;
    private byte[] body;
    private int responseCode = ResponseCode.SUCCESS;
    private String remark;

    public ControllerResult() {
        this(null);
    }

    public ControllerResult(T response) {
        this.events = new ArrayList<>();
        this.response = response;
    }

    public ControllerResult(List<EventMessage> events, T response) {
        this.events = new ArrayList<>(events);
        this.response = response;
    }

    public static <T> ControllerResult<T> of(List<EventMessage> events, T response) {
        return new ControllerResult<>(events, response);
    }

    public List<EventMessage> getEvents() {
        return new ArrayList<>(events);
    }

    public T getResponse() {
        return response;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public void setCodeAndRemark(int responseCode, String remark) {
        this.responseCode = responseCode;
        this.remark = remark;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getRemark() {
        return remark;
    }

    public void addEvent(EventMessage event) {
        this.events.add(event);
    }

    @Override
    public String toString() {
        return "ControllerResult{" +
            "events=" + events +
            ", response=" + response +
            '}';
    }
}
