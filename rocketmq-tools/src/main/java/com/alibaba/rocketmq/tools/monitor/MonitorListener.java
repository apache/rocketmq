/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.tools.monitor;

import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;

import java.util.TreeMap;


/**
 * 监控监听器
 */
public interface MonitorListener {
    /**
     * 开始一轮监控
     */
    public void beginRound();


    /**
     * 汇报消息堆积情况
     */
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs);


    /**
     * 汇报消费失败情况
     */
    public void reportFailedMsgs(FailedMsgs failedMsgs);


    /**
     * 汇报消息删除情况
     */
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent);


    /**
     * 汇报Consumer内部运行数据结构
     */
    public void reportConsumerRunningInfo(TreeMap<String/* clientId */, ConsumerRunningInfo> criTable);


    /**
     * 结束一轮监控
     */
    public void endRound();
}
