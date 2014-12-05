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
package com.alibaba.rocketmq.store;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 存储层对外提供的接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public interface MessageStore {

    /**
     * 重启时，加载数据
     */
    public boolean load();


    /**
     * 启动服务
     */
    public void start() throws Exception;


    /**
     * 关闭服务
     */
    public void shutdown();


    /**
     * 删除所有文件，单元测试会使用
     */
    public void destroy();


    /**
     * 存储消息
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg);


    /**
     * 读取消息，如果types为null，则不做过滤
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId,
            final long offset, final int maxMsgNums, final SubscriptionData subscriptionData);


    /**
     * 获取指定队列最大Offset 如果队列不存在，返回-1
     */
    public long getMaxOffsetInQuque(final String topic, final int queueId);


    /**
     * 获取指定队列最小Offset 如果队列不存在，返回-1
     */
    public long getMinOffsetInQuque(final String topic, final int queueId);


    /**
     * 获取消费队列记录的CommitLog Offset
     */
    public long getCommitLogOffsetInQueue(final String topic, final int queueId, final long cqOffset);


    /**
     * 根据消息时间获取某个队列中对应的offset 1、如果指定时间（包含之前之后）有对应的消息，则获取距离此时间最近的offset（优先选择之前）
     * 2、如果指定时间无对应消息，则返回0
     */
    public long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);


    /**
     * 通过物理队列Offset，查询消息。 如果发生错误，则返回null
     */
    public MessageExt lookMessageByOffset(final long commitLogOffset);


    /**
     * 通过物理队列Offset，查询消息。 如果发生错误，则返回null
     */
    public SelectMapedBufferResult selectOneMessageByOffset(final long commitLogOffset);


    public SelectMapedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);


    /**
     * 获取运行时统计数据
     */
    public String getRunningDataInfo();


    /**
     * 获取运行时统计数据
     */
    public HashMap<String, String> getRuntimeInfo();


    /**
     * 获取物理队列最大offset
     */
    public long getMaxPhyOffset();


    public long getMinPhyOffset();


    /**
     * 获取队列中最早的消息时间
     */
    public long getEarliestMessageTime(final String topic, final int queueId);


    public long getMessageStoreTimeStamp(final String topic, final int queueId, final long offset);


    /**
     * 获取队列中的消息总数
     */
    public long getMessageTotalInQueue(final String topic, final int queueId);


    /**
     * 数据复制使用：获取CommitLog数据
     */
    public SelectMapedBufferResult getCommitLogData(final long offset);


    /**
     * 数据复制使用：向CommitLog追加数据，并分发至各个Consume Queue
     */
    public boolean appendToCommitLog(final long startOffset, final byte[] data);


    /**
     * 手动触发删除文件
     */
    public void excuteDeleteFilesManualy();


    /**
     * 根据消息Key查询消息
     */
    public QueryMessageResult queryMessage(final String topic, final String key, final int maxNum,
            final long begin, final long end);


    public void updateHaMasterAddress(final String newAddr);


    /**
     * Slave落后Master多少，单位字节
     */
    public long slaveFallBehindMuch();


    public long now();


    public int cleanUnusedTopic(final Set<String> topics);


    /**
     * 清除失效的消费队列
     */
    public void cleanExpiredConsumerQueue();


    /**
     * 批量获取 messageId
     */
    public Map<String, Long> getMessageIds(final String topic, int queueId, long minOffset,
            final long maxOffset, SocketAddress storeHost);


    /**
     * 判断消息是否在磁盘
     */
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);
}
