package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "public class MQFaultStrategy {\n" +
            "    private final static Logger log = ClientLogger.getLog();\n" +
            "\n" +
            "    /**\n" +
            "     * 延迟故障容错，维护每个Broker的发送消息的延迟\n" +
            "     * key：brokerName\n" +
            "     */\n" +
            "    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();\n" +
            "    /**\n" +
            "     * 发送消息延迟容错开关\n" +
            "     */\n" +
            "    private boolean sendLatencyFaultEnable = false;\n" +
            "    /**\n" +
            "     * 延迟级别数组\n" +
            "     */\n" +
            "    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};\n" +
            "    /**\n" +
            "     * 不可用时长数组\n" +
            "     */\n" +
            "    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};\n" +
            "\n" +
            "    /**\n" +
            "     * 根据 Topic发布信息 选择一个消息队列\n" +
            "     *\n" +
            "     * @param tpInfo Topic发布信息\n" +
            "     * @param lastBrokerName brokerName\n" +
            "     * @return 消息队列\n" +
            "     */\n" +
            "    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {\n" +
            "        if (this.sendLatencyFaultEnable) {\n" +
            "            try {\n" +
            "                // 获取 brokerName=lastBrokerName && 可用的一个消息队列\n" +
            "                int index = tpInfo.getSendWhichQueue().getAndIncrement();\n" +
            "                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {\n" +
            "                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();\n" +
            "                    if (pos < 0)\n" +
            "                        pos = 0;\n" +
            "                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);\n" +
            "                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {\n" +
            "                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))\n" +
            "                            return mq;\n" +
            "                    }\n" +
            "                }\n" +
            "                // 选择一个相对好的broker，并获得其对应的一个消息队列，不考虑该队列的可用性\n" +
            "                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();\n" +
            "                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);\n" +
            "                if (writeQueueNums > 0) {\n" +
            "                    final MessageQueue mq = tpInfo.selectOneMessageQueue();\n" +
            "                    if (notBestBroker != null) {\n" +
            "                        mq.setBrokerName(notBestBroker);\n" +
            "                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);\n" +
            "                    }\n" +
            "                    return mq;\n" +
            "                } else {\n" +
            "                    latencyFaultTolerance.remove(notBestBroker);\n" +
            "                }\n" +
            "            } catch (Exception e) {\n" +
            "                log.error(\"Error occurred when selecting message queue\", e);\n" +
            "            }\n" +
            "            // 选择一个消息队列，不考虑队列的可用性\n" +
            "            return tpInfo.selectOneMessageQueue();\n" +
            "        }\n" +
            "        // 获得 lastBrokerName 对应的一个消息队列，不考虑该队列的可用性\n" +
            "        return tpInfo.selectOneMessageQueue(lastBrokerName);\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 更新延迟容错信息\n" +
            "     *\n" +
            "     * @param brokerName brokerName\n" +
            "     * @param currentLatency 延迟\n" +
            "     * @param isolation 是否隔离。当开启隔离时，默认延迟为30000。目前主要用于发送消息异常时\n" +
            "     */\n" +
            "    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {\n" +
            "        if (this.sendLatencyFaultEnable) {\n" +
            "            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);\n" +
            "            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 计算延迟对应的不可用时间\n" +
            "     *\n" +
            "     * @param currentLatency 延迟\n" +
            "     * @return 不可用时间\n" +
            "     */\n" +
            "    private long computeNotAvailableDuration(final long currentLatency) {\n" +
            "        for (int i = latencyMax.length - 1; i >= 0; i--) {\n" +
            "            if (currentLatency >= latencyMax[i])\n" +
            "                return this.notAvailableDuration[i];\n" +
            "        }\n" +
            "        return 0;\n" +
            "    }\n" +
            "\n" +
            "    public long[] getNotAvailableDuration() {\n" +
            "        return notAvailableDuration;\n" +
            "    }\n" +
            "\n" +
            "    public void setNotAvailableDuration(final long[] notAvailableDuration) {\n" +
            "        this.notAvailableDuration = notAvailableDuration;\n" +
            "    }\n" +
            "\n" +
            "    public long[] getLatencyMax() {\n" +
            "        return latencyMax;\n" +
            "    }\n" +
            "\n" +
            "    public void setLatencyMax(final long[] latencyMax) {\n" +
            "        this.latencyMax = latencyMax;\n" +
            "    }\n" +
            "\n" +
            "    public boolean isSendLatencyFaultEnable() {\n" +
            "        return sendLatencyFaultEnable;\n" +
            "    }\n" +
            "\n" +
            "    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {\n" +
            "        this.sendLatencyFaultEnable = sendLatencyFaultEnable;\n" +
            "    }\n" +
            "}";

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
