package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "    /**\n" +
            "     * 处理拉取结果\n" +
            "     * 1. 更新消息队列拉取消息Broker编号的映射\n" +
            "     * 2. 解析消息，并根据消息tagCode匹配合适消息\n" +
            "     *\n" +
            "     * @param mq 消息队列\n" +
            "     * @param pullResult 拉取结果\n" +
            "     * @param subscriptionData 订阅信息\n" +
            "     * @return 拉取结果\n" +
            "     */\n" +
            "    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,\n" +
            "        final SubscriptionData subscriptionData) {\n" +
            "        PullResultExt pullResultExt = (PullResultExt) pullResult;\n" +
            "\n" +
            "        // 更新消息队列拉取消息Broker编号的映射\n" +
            "        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());\n" +
            "\n" +
            "        // 解析消息，并根据消息tagCode匹配合适消息\n" +
            "        if (PullStatus.FOUND == pullResult.getPullStatus()) {\n" +
            "            // 解析消息\n" +
            "            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());\n" +
            "            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);\n" +
            "\n" +
            "            // 根据消息tagCode匹配合适消息\n" +
            "            List<MessageExt> msgListFilterAgain = msgList;\n" +
            "            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {\n" +
            "                msgListFilterAgain = new ArrayList<>(msgList.size());\n" +
            "                for (MessageExt msg : msgList) {\n" +
            "                    if (msg.getTags() != null) {\n" +
            "                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {\n" +
            "                            msgListFilterAgain.add(msg);\n" +
            "                        }\n" +
            "                    }\n" +
            "                }\n" +
            "            }\n" +
            "\n" +
            "            // Hook\n" +
            "            if (this.hasHook()) {\n" +
            "                FilterMessageContext filterMessageContext = new FilterMessageContext();\n" +
            "                filterMessageContext.setUnitMode(unitMode);\n" +
            "                filterMessageContext.setMsgList(msgListFilterAgain);\n" +
            "                this.executeHook(filterMessageContext);\n" +
            "            }\n" +
            "\n" +
            "            // 设置消息队列当前最小/最大位置到消息拓展字段\n" +
            "            for (MessageExt msg : msgListFilterAgain) {\n" +
            "                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,\n" +
            "                    Long.toString(pullResult.getMinOffset()));\n" +
            "                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,\n" +
            "                    Long.toString(pullResult.getMaxOffset()));\n" +
            "            }\n" +
            "\n" +
            "            // 设置消息列表\n" +
            "            pullResultExt.setMsgFoundList(msgListFilterAgain);\n" +
            "        }\n" +
            "\n" +
            "        // 清空消息二进制数组\n" +
            "        pullResultExt.setMessageBinary(null);\n" +
            "\n" +
            "        return pullResult;\n" +
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
