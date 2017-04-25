package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "    /**\n" +
            "     * 获取消息结果\n" +
            "     *\n" +
            "     * @param group 消费分组\n" +
            "     * @param topic 主题\n" +
            "     * @param queueId 队列编号\n" +
            "     * @param offset 队列位置\n" +
            "     * @param maxMsgNums 消息数量\n" +
            "     * @param subscriptionData 订阅信息\n" +
            "     * @return 消息结果\n" +
            "     */\n" +
            "    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums,\n" +
            "        final SubscriptionData subscriptionData) {\n" +
            "        // 是否关闭\n" +
            "        if (this.shutdown) {\n" +
            "            log.warn(\"message store has shutdown, so getMessage is forbidden\");\n" +
            "            return null;\n" +
            "        }\n" +
            "        // 是否可读\n" +
            "        if (!this.runningFlags.isReadable()) {\n" +
            "            log.warn(\"message store is not readable, so getMessage is forbidden \" + this.runningFlags.getFlagBits());\n" +
            "            return null;\n" +
            "        }\n" +
            "\n" +
            "        long beginTime = this.getSystemClock().now();\n" +
            "\n" +
            "        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;\n" +
            "        long nextBeginOffset = offset;\n" +
            "        long minOffset = 0;\n" +
            "        long maxOffset = 0;\n" +
            "\n" +
            "        GetMessageResult getResult = new GetMessageResult();\n" +
            "\n" +
            "        final long maxOffsetPy = this.commitLog.getMaxOffset();\n" +
            "\n" +
            "        // 获取消费队列\n" +
            "        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);\n" +
            "        if (consumeQueue != null) {\n" +
            "            minOffset = consumeQueue.getMinOffsetInQueue(); // 消费队列 最小队列编号\n" +
            "            maxOffset = consumeQueue.getMaxOffsetInQueue(); // 消费队列 最大队列编号\n" +
            "\n" +
            "            // 判断 队列位置(offset)\n" +
            "            if (maxOffset == 0) { // 消费队列无消息\n" +
            "                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;\n" +
            "                nextBeginOffset = nextOffsetCorrection(offset, 0);\n" +
            "            } else if (offset < minOffset) { // 查询offset 太小\n" +
            "                status = GetMessageStatus.OFFSET_TOO_SMALL;\n" +
            "                nextBeginOffset = nextOffsetCorrection(offset, minOffset);\n" +
            "            } else if (offset == maxOffset) { // 查询offset 超过 消费队列 一个位置\n" +
            "                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;\n" +
            "                nextBeginOffset = nextOffsetCorrection(offset, offset);\n" +
            "            } else if (offset > maxOffset) { // 查询offset 超过 消费队列 太多(大于一个位置)\n" +
            "                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;\n" +
            "                if (0 == minOffset) { // TODO blog 这里是？？为啥0 == minOffset做了特殊判断\n" +
            "                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);\n" +
            "                } else {\n" +
            "                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);\n" +
            "                }\n" +
            "            } else {\n" +
            "                // 获得 映射Buffer结果(MappedFile)\n" +
            "                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);\n" +
            "                if (bufferConsumeQueue != null) {\n" +
            "                    try {\n" +
            "                        status = GetMessageStatus.NO_MATCHED_MESSAGE;\n" +
            "\n" +
            "                        long nextPhyFileStartOffset = Long.MIN_VALUE; // commitLog下一个文件(MappedFile)对应的开始offset。\n" +
            "                        long maxPhyOffsetPulling = 0; // 消息物理位置拉取到的最大offset\n" +
            "\n" +
            "                        int i = 0;\n" +
            "                        final int maxFilterMessageCount = 16000;\n" +
            "                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();\n" +
            "                        // 循环获取 消息位置信息\n" +
            "                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {\n" +
            "                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong(); // 消息物理位置offset\n" +
            "                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt(); // 消息长度\n" +
            "                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong(); // 消息tagsCode\n" +
            "                            // 设置消息物理位置拉取到的最大offset\n" +
            "                            maxPhyOffsetPulling = offsetPy;\n" +
            "                            // 当 offsetPy 小于 nextPhyFileStartOffset 时，意味着对应的 Message 已经移除，所以直接continue，直到可读取的Message。\n" +
            "                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {\n" +
            "                                if (offsetPy < nextPhyFileStartOffset)\n" +
            "                                    continue;\n" +
            "                            }\n" +
            "                            // 校验 commitLog 是否需要硬盘，无法全部放在内存\n" +
            "                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);\n" +
            "                            // 是否已经获得足够消息\n" +
            "                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),\n" +
            "                                isInDisk)) {\n" +
            "                                break;\n" +
            "                            }\n" +
            "                            // 判断消息是否符合条件\n" +
            "                            if (this.messageFilter.isMessageMatched(subscriptionData, tagsCode)) {\n" +
            "                                // 从commitLog获取对应消息ByteBuffer\n" +
            "                                SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);\n" +
            "                                if (selectResult != null) {\n" +
            "                                    this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();\n" +
            "                                    getResult.addMessage(selectResult);\n" +
            "                                    status = GetMessageStatus.FOUND;\n" +
            "                                    nextPhyFileStartOffset = Long.MIN_VALUE;\n" +
            "                                } else {\n" +
            "                                    // 从commitLog无法读取到消息，说明该消息对应的文件（MappedFile）已经删除，计算下一个MappedFile的起始位置\n" +
            "                                    if (getResult.getBufferTotalSize() == 0) {\n" +
            "                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;\n" +
            "                                    }\n" +
            "                                    nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);\n" +
            "                                }\n" +
            "                            } else {\n" +
            "                                if (getResult.getBufferTotalSize() == 0) {\n" +
            "                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;\n" +
            "                                }\n" +
            "\n" +
            "                                if (log.isDebugEnabled()) {\n" +
            "                                    log.debug(\"message type not matched, client: \" + subscriptionData + \" server: \" + tagsCode);\n" +
            "                                }\n" +
            "                            }\n" +
            "                        }\n" +
            "                        // 统计剩余可拉取消息字节数\n" +
            "                        if (diskFallRecorded) {\n" +
            "                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;\n" +
            "                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);\n" +
            "                        }\n" +
            "                        // 计算下次拉取消息的消息队列编号\n" +
            "                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);\n" +
            "                        // 根据剩余可拉取消息字节数与内存判断是否建议读取从节点\n" +
            "                        long diff = maxOffsetPy - maxPhyOffsetPulling;\n" +
            "                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE\n" +
            "                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));\n" +
            "                        getResult.setSuggestPullingFromSlave(diff > memory);\n" +
            "                    } finally {\n" +
            "                        bufferConsumeQueue.release();\n" +
            "                    }\n" +
            "                } else {\n" +
            "                    status = GetMessageStatus.OFFSET_FOUND_NULL;\n" +
            "                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));\n" +
            "                    log.warn(\"consumer request topic: \" + topic + \"offset: \" + offset + \" minOffset: \" + minOffset + \" maxOffset: \"\n" +
            "                        + maxOffset + \", but access logic queue failed.\");\n" +
            "                }\n" +
            "            }\n" +
            "        } else {\n" +
            "            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;\n" +
            "            nextBeginOffset = nextOffsetCorrection(offset, 0);\n" +
            "        }\n" +
            "        // 统计\n" +
            "        if (GetMessageStatus.FOUND == status) {\n" +
            "            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();\n" +
            "        } else {\n" +
            "            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();\n" +
            "        }\n" +
            "        long eclipseTime = this.getSystemClock().now() - beginTime;\n" +
            "        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);\n" +
            "        // 设置返回结果\n" +
            "        getResult.setStatus(status);\n" +
            "        getResult.setNextBeginOffset(nextBeginOffset);\n" +
            "        getResult.setMaxOffset(maxOffset);\n" +
            "        getResult.setMinOffset(minOffset);\n" +
            "        return getResult;\n" +
            "    }"
            + "\n"
            + "\n"
            + "    /**\n" +
            "     * 根据 主题 + 队列编号 获取 消费队列\n" +
            "     *\n" +
            "     * @param topic 主题\n" +
            "     * @param queueId 队列编号\n" +
            "     * @return 消费队列\n" +
            "     */\n" +
            "    public ConsumeQueue findConsumeQueue(String topic, int queueId) {\n" +
            "        // 获取 topic 对应的 所有消费队列\n" +
            "        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);\n" +
            "        if (null == map) {\n" +
            "            ConcurrentHashMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<>(128);\n" +
            "            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);\n" +
            "            if (oldMap != null) {\n" +
            "                map = oldMap;\n" +
            "            } else {\n" +
            "                map = newMap;\n" +
            "            }\n" +
            "        }\n" +
            "        // 获取 queueId 对应的 消费队列\n" +
            "        ConsumeQueue logic = map.get(queueId);\n" +
            "        if (null == logic) {\n" +
            "            ConsumeQueue newLogic = new ConsumeQueue(//\n" +
            "                topic, //\n" +
            "                queueId, //\n" +
            "                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), //\n" +
            "                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(), //\n" +
            "                this);\n" +
            "            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);\n" +
            "            if (oldLogic != null) {\n" +
            "                logic = oldLogic;\n" +
            "            } else {\n" +
            "                logic = newLogic;\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        return logic;\n" +
            "    }\n"
            + "\n"
            + "    /**\n" +
            "     * 下一个获取队列offset修正\n" +
            "     * 修正条件：主节点 或者 从节点开启校验offset开关\n" +
            "     *\n" +
            "     * @param oldOffset 老队列offset\n" +
            "     * @param newOffset 新队列offset\n" +
            "     * @return 修正后的队列offset\n" +
            "     */\n" +
            "    private long nextOffsetCorrection(long oldOffset, long newOffset) {\n" +
            "        long nextOffset = oldOffset;\n" +
            "        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {\n" +
            "            nextOffset = newOffset;\n" +
            "        }\n" +
            "        return nextOffset;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 校验 commitLog 是否需要硬盘，无法全部放在内存\n" +
            "     *\n" +
            "     * @param offsetPy commitLog 指定offset\n" +
            "     * @param maxOffsetPy commitLog 最大offset\n" +
            "     * @return 是否需要硬盘\n" +
            "     */\n" +
            "    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {\n" +
            "        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));\n" +
            "        return (maxOffsetPy - offsetPy) > memory;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 判断获取消息是否已经满\n" +
            "     *\n" +
            "     * @param sizePy 字节数\n" +
            "     * @param maxMsgNums 最大消息数\n" +
            "     * @param bufferTotal 目前已经计算字节数\n" +
            "     * @param messageTotal 目前已经计算消息数\n" +
            "     * @param isInDisk 是否在硬盘中\n" +
            "     * @return 是否已满\n" +
            "     */\n" +
            "    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {\n" +
            "        if (0 == bufferTotal || 0 == messageTotal) {\n" +
            "            return false;\n" +
            "        }\n" +
            "        // 消息数量已经满足请求数量(maxMsgNums)\n" +
            "        if ((messageTotal + 1) >= maxMsgNums) {\n" +
            "            return true;\n" +
            "        }\n" +
            "        // 根据消息存储配置的最大传输字节数、最大传输消息数是否已满\n" +
            "        if (isInDisk) {\n" +
            "            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {\n" +
            "                return true;\n" +
            "            }\n" +
            "\n" +
            "            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk()) {\n" +
            "                return true;\n" +
            "            }\n" +
            "        } else {\n" +
            "            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {\n" +
            "                return true;\n" +
            "            }\n" +
            "\n" +
            "            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory()) {\n" +
            "                return true;\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        return false;\n" +
            "    }"
            ;

    public static void main(String[] args) {
        int i = 1;
        boolean replaceBlank = STR.split("\n")[0].contains("class")
                || STR.split("\n")[0].contains("interface");
        for (String str : STR.split("\n")) {
            if (!replaceBlank) {
                str = str.replaceFirst("    ", "");
            }
            if (i < 10) {
                System.out.print("  " + i + ": ");
            } else if (i < 100) {
                System.out.print(" " + i + ": ");
            } else {
                System.out.print("" + i + ": ");
            }
            System.out.println(str);
            i++;
        }
    }

}
