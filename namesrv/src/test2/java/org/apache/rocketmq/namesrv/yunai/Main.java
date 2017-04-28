package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "    /**\n" +
            "     * 添加位置信息封装\n" +
            "     *\n" +
            "     * @param offset commitLog存储位置\n" +
            "     * @param size 消息长度\n" +
            "     * @param tagsCode 消息tagsCode\n" +
            "     * @param storeTimestamp 消息存储时间\n" +
            "     * @param logicOffset 队列位置\n" +
            "     */\n" +
            "    public void putMessagePositionInfoWrapper(long offset, int size, long tagsCode, long storeTimestamp,\n" +
            "        long logicOffset) {\n" +
            "        final int maxRetries = 30;\n" +
            "        boolean canWrite = this.defaultMessageStore.getRunningFlags().isWriteable();\n" +
            "        // 多次循环写，直到成功\n" +
            "        for (int i = 0; i < maxRetries && canWrite; i++) {\n" +
            "            // 调用添加位置信息\n" +
            "            boolean result = this.putMessagePositionInfo(offset, size, tagsCode, logicOffset);\n" +
            "            if (result) {\n" +
            "                // 添加成功，使用消息存储时间 作为 存储check point。\n" +
            "                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);\n" +
            "                return;\n" +
            "            } else {\n" +
            "                // XXX: warn and notify me\n" +
            "                log.warn(\"[BUG]put commit log position info to \" + topic + \":\" + queueId + \" \" + offset\n" +
            "                    + \" failed, retry \" + i + \" times\");\n" +
            "\n" +
            "                try {\n" +
            "                    Thread.sleep(1000);\n" +
            "                } catch (InterruptedException e) {\n" +
            "                    log.warn(\"\", e);\n" +
            "                }\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        // XXX: warn and notify me 设置异常不可写入\n" +
            "        log.error(\"[BUG]consume queue can not write, {} {}\", this.topic, this.queueId);\n" +
            "        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 添加位置信息，并返回添加是否成功\n" +
            "     *\n" +
            "     * @param offset commitLog存储位置\n" +
            "     * @param size 消息长度\n" +
            "     * @param tagsCode 消息tagsCode\n" +
            "     * @param cqOffset 队列位置\n" +
            "     * @return 是否成功\n" +
            "     */\n" +
            "    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,\n" +
            "        final long cqOffset) {\n" +
            "        // 如果已经重放过，直接返回成功\n" +
            "        if (offset <= this.maxPhysicOffset) {\n" +
            "            return true;\n" +
            "        }\n" +
            "        // 写入位置信息到byteBuffer\n" +
            "        this.byteBufferIndex.flip();\n" +
            "        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);\n" +
            "        this.byteBufferIndex.putLong(offset);\n" +
            "        this.byteBufferIndex.putInt(size);\n" +
            "        this.byteBufferIndex.putLong(tagsCode);\n" +
            "        // 计算consumeQueue存储位置，并获得对应的MappedFile\n" +
            "        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;\n" +
            "        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);\n" +
            "        if (mappedFile != null) {\n" +
            "            // 当是ConsumeQueue第一个MappedFile && 队列位置非第一个 && MappedFile未写入内容，则填充前置空白占位\n" +
            "            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) { // TODO 疑问：为啥这个操作。目前能够想象到的是，一些老的消息很久没发送，突然发送，这个时候刚好满足。\n" +
            "                this.minLogicOffset = expectLogicOffset;\n" +
            "                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);\n" +
            "                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);\n" +
            "                this.fillPreBlank(mappedFile, expectLogicOffset);\n" +
            "                log.info(\"fill pre blank space \" + mappedFile.getFileName() + \" \" + expectLogicOffset + \" \"\n" +
            "                    + mappedFile.getWrotePosition());\n" +
            "            }\n" +
            "            // 校验consumeQueue存储位置是否合法。TODO 如果不合法，继续写入会不会有问题？\n" +
            "            if (cqOffset != 0) {\n" +
            "                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();\n" +
            "                if (expectLogicOffset != currentLogicOffset) {\n" +
            "                    LOG_ERROR.warn(\n" +
            "                        \"[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}\",\n" +
            "                        expectLogicOffset,\n" +
            "                        currentLogicOffset,\n" +
            "                        this.topic,\n" +
            "                        this.queueId,\n" +
            "                        expectLogicOffset - currentLogicOffset\n" +
            "                    );\n" +
            "                }\n" +
            "            }\n" +
            "            // 设置commitLog重放消息到ConsumeQueue位置。\n" +
            "            this.maxPhysicOffset = offset;\n" +
            "            // 插入mappedFile\n" +
            "            return mappedFile.appendMessage(this.byteBufferIndex.array());\n" +
            "        }\n" +
            "        return false;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 填充前置空白占位\n" +
            "     *\n" +
            "     * @param mappedFile MappedFile\n" +
            "     * @param untilWhere consumeQueue存储位置\n" +
            "     */\n" +
            "    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {\n" +
            "        // 写入前置空白占位到byteBuffer\n" +
            "        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);\n" +
            "        byteBuffer.putLong(0L);\n" +
            "        byteBuffer.putInt(Integer.MAX_VALUE);\n" +
            "        byteBuffer.putLong(0L);\n" +
            "        // 循环填空\n" +
            "        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());\n" +
            "        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {\n" +
            "            mappedFile.appendMessage(byteBuffer.array());\n" +
            "        }\n" +
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
