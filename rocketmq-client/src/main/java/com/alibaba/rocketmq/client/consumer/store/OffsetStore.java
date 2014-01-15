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
package com.alibaba.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * Consumer Offset存储接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public interface OffsetStore {
    /**
     * 加载Offset
     * 
     * @throws MQClientException
     */
    public void load() throws MQClientException;


    /**
     * 更新消费进度，存储到内存
     */
    public void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);


    /**
     * 从本地缓存读取消费进度
     */
    public long readOffset(final MessageQueue mq, final ReadOffsetType type);


    /**
     * 持久化全部消费进度，可能持久化本地或者远端Broker
     */
    public void persistAll(final Set<MessageQueue> mqs);


    public void persist(final MessageQueue mq);


    /**
     * 删除不必要的MessageQueue offset
     */
    public void removeOffset(MessageQueue mq);


    /**
     * 如果 topic 为空，则不对 topic 进行过滤，全部拷贝。
     */
    public Map<MessageQueue, Long> cloneOffsetTable(String topic);
}
