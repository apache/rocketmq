/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer;

public class PullTaskContext {
    /**
     * 距离下次拉取这个队列的间隔时间，单位毫秒
     */
    private int pullNextDelayTimeMillis = 200;


    public int getPullNextDelayTimeMillis() {
        return pullNextDelayTimeMillis;
    }


    public void setPullNextDelayTimeMillis(int pullNextDelayTimeMillis) {
        this.pullNextDelayTimeMillis = pullNextDelayTimeMillis;
    }
}
