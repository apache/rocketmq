package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR =
             "// 【DefaultMQProducerImpl.java】\n" +

            "    private SendResult sendDefaultImpl(//\n" +
                     "        Message msg, //\n" +
                     "        final CommunicationMode communicationMode, //\n" +
                     "        final SendCallback sendCallback, //\n" +
                     "        final long timeout//\n" +
                     "    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {\n" +
                     "        // .... 省略：处理【校验逻辑】\n" +
                     "        // 获取 Topic路由信息\n" +
                     "        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());\n" +
                     "        if (topicPublishInfo != null && topicPublishInfo.ok()) {\n" +
                     "            MessageQueue mq = null; // 最后选择消息要发送到的队列\n" +
                     "            Exception exception = null;\n" +
                     "            SendResult sendResult = null; // 最后一次发送结果\n" +
                     "            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1; // 同步多次调用\n" +
                     "            int times = 0; // 第几次发送\n" +
                     "            String[] brokersSent = new String[timesTotal]; // 存储每次发送消息选择的broker名\n" +
                     "            // 循环调用发送消息，直到成功\n" +
                     "            for (; times < timesTotal; times++) {\n" +
                     "                String lastBrokerName = null == mq ? null : mq.getBrokerName();\n" +
                     "                MessageQueue tmpmq = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName); // 选择消息要发送到的队列\n" +
                     "                if (tmpmq != null) {\n" +
                     "                    mq = tmpmq;\n" +
                     "                    brokersSent[times] = mq.getBrokerName();\n" +
                     "                    try {\n" +
                     "                        beginTimestampPrev = System.currentTimeMillis();\n" +
                     "                        // 调用发送消息核心方法\n" +
                     "                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout);\n" +
                     "                        endTimestamp = System.currentTimeMillis();\n" +
                     "                        // 更新Broker可用性信息\n" +
                     "                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);\n" +
                     "                        // .... 省略：处理【发送返回结果】\n" +
                     "                        }\n" +
                     "                    } catch (e) { // .... 省略：处理【异常】\n" +
                     "                        \n" +
                     "                    }\n" +
                     "                } else {\n" +
                     "                    break;\n" +
                     "                }\n" +
                     "            }\n" +
                     "            // .... 省略：处理【发送返回结果】\n" +
                     "        }\n" +
                     "        // .... 省略：处理【找不到消息路由】\n" +
                     "    }"            ;

    public static void main(String[] args) {
        int i = 1;
        boolean replaceBlank = STR.split("\n")[0].contains("class")
                || STR.split("\n")[0].contains("interface");
        for (String str : STR.split("\n")) {
            if (!replaceBlank) {
                str = str.replaceFirst("    ", "");
//                str = str.replaceFirst("        ", "");
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
