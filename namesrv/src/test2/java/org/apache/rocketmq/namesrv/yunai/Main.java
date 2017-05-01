package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)\n" +
            "        throws RemotingCommandException {\n" +
            "\n" +
            "        // 初始化响应\n" +
            "        final RemotingCommand response = RemotingCommand.createResponseCommand(null);\n" +
            "        final ConsumerSendMsgBackRequestHeader requestHeader =\n" +
            "            (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);\n" +
            "\n" +
            "        // hook（独有）\n" +
            "        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {\n" +
            "\n" +
            "            ConsumeMessageContext context = new ConsumeMessageContext();\n" +
            "            context.setConsumerGroup(requestHeader.getGroup());\n" +
            "            context.setTopic(requestHeader.getOriginTopic());\n" +
            "            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);\n" +
            "            context.setCommercialRcvTimes(1);\n" +
            "            context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));\n" +
            "\n" +
            "            this.executeConsumeMessageHookAfter(context);\n" +
            "        }\n" +
            "\n" +
            "        // 判断消费分组是否存在（独有）\n" +
            "        SubscriptionGroupConfig subscriptionGroupConfig =\n" +
            "            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());\n" +
            "        if (null == subscriptionGroupConfig) {\n" +
            "            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);\n" +
            "            response.setRemark(\"subscription group not exist, \" + requestHeader.getGroup() + \" \"\n" +
            "                + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        // 检查 broker 是否有写入权限\n" +
            "        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {\n" +
            "            response.setCode(ResponseCode.NO_PERMISSION);\n" +
            "            response.setRemark(\"the broker[\" + this.brokerController.getBrokerConfig().getBrokerIP1() + \"] sending message is forbidden\");\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        // 检查 重试队列数 是否大于0（独有）\n" +
            "        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {\n" +
            "            response.setCode(ResponseCode.SUCCESS);\n" +
            "            response.setRemark(null);\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        // 计算retry Topic\n" +
            "        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());\n" +
            "\n" +
            "        // 计算队列编号（独有）\n" +
            "        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();\n" +
            "\n" +
            "        // 计算sysFlag（独有）\n" +
            "        int topicSysFlag = 0;\n" +
            "        if (requestHeader.isUnitMode()) {\n" +
            "            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);\n" +
            "        }\n" +
            "\n" +
            "        // 获取topicConfig。如果获取不到，则进行创建\n" +
            "        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//\n" +
            "            newTopic, //\n" +
            "            subscriptionGroupConfig.getRetryQueueNums(), //\n" +
            "            PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);\n" +
            "        if (null == topicConfig) { // 没有配置\n" +
            "            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
            "            response.setRemark(\"topic[\" + newTopic + \"] not exist\");\n" +
            "            return response;\n" +
            "        }\n" +
            "        if (!PermName.isWriteable(topicConfig.getPerm())) { // 不允许写入\n" +
            "            response.setCode(ResponseCode.NO_PERMISSION);\n" +
            "            response.setRemark(String.format(\"the topic[%s] sending message is forbidden\", newTopic));\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        // 查询消息。若不存在，返回异常错误。（独有）\n" +
            "        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());\n" +
            "        if (null == msgExt) {\n" +
            "            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
            "            response.setRemark(\"look message by offset failed, \" + requestHeader.getOffset());\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        // 设置retryTopic到拓展属性（独有）\n" +
            "        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);\n" +
            "        if (null == retryTopic) {\n" +
            "            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());\n" +
            "        }\n" +
            "\n" +
            "        // 设置消息不等待存储完成（独有） TODO 疑问：如果设置成不等待存储，broker设置成同步落盘，岂不是不能批量提交了？\n" +
            "        msgExt.setWaitStoreMsgOK(false);\n" +
            "\n" +
            "        // 处理 delayLevel（独有）。\n" +
            "        int delayLevel = requestHeader.getDelayLevel();\n" +
            "        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();\n" +
            "        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {\n" +
            "            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();\n" +
            "        }\n" +
            "        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes//\n" +
            "            || delayLevel < 0) { // 如果超过最大消费次数，则topic修改成\"%DLQ%\" + 分组名，即加入 死信队列(Dead Letter Queue)\n" +
            "            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());\n" +
            "            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;\n" +
            "\n" +
            "            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, //\n" +
            "                DLQ_NUMS_PER_GROUP, //\n" +
            "                PermName.PERM_WRITE, 0\n" +
            "            );\n" +
            "            if (null == topicConfig) {\n" +
            "                response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
            "                response.setRemark(\"topic[\" + newTopic + \"] not exist\");\n" +
            "                return response;\n" +
            "            }\n" +
            "        } else {\n" +
            "            if (0 == delayLevel) {\n" +
            "                delayLevel = 3 + msgExt.getReconsumeTimes();\n" +
            "            }\n" +
            "            msgExt.setDelayTimeLevel(delayLevel);\n" +
            "        }\n" +
            "\n" +
            "        // 创建MessageExtBrokerInner\n" +
            "        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();\n" +
            "        msgInner.setTopic(newTopic);\n" +
            "        msgInner.setBody(msgExt.getBody());\n" +
            "        msgInner.setFlag(msgExt.getFlag());\n" +
            "        MessageAccessor.setProperties(msgInner, msgExt.getProperties());\n" +
            "        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));\n" +
            "        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));\n" +
            "        msgInner.setQueueId(queueIdInt);\n" +
            "        msgInner.setSysFlag(msgExt.getSysFlag());\n" +
            "        msgInner.setBornTimestamp(msgExt.getBornTimestamp());\n" +
            "        msgInner.setBornHost(msgExt.getBornHost());\n" +
            "        msgInner.setStoreHost(this.getStoreHost());\n" +
            "        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);\n" +
            "\n" +
            "        // 设置原始消息编号到拓展字段（独有）\n" +
            "        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);\n" +
            "        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);\n" +
            "\n" +
            "        // 添加消息\n" +
            "        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);\n" +
            "        if (putMessageResult != null) {\n" +
            "            switch (putMessageResult.getPutMessageStatus()) {\n" +
            "                case PUT_OK:\n" +
            "                    String backTopic = msgExt.getTopic();\n" +
            "                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);\n" +
            "                    if (correctTopic != null) {\n" +
            "                        backTopic = correctTopic;\n" +
            "                    }\n" +
            "\n" +
            "                    this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);\n" +
            "\n" +
            "                    response.setCode(ResponseCode.SUCCESS);\n" +
            "                    response.setRemark(null);\n" +
            "\n" +
            "                    return response;\n" +
            "                default:\n" +
            "                    break;\n" +
            "            }\n" +
            "\n" +
            "            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
            "            response.setRemark(putMessageResult.getPutMessageStatus().name());\n" +
            "            return response;\n" +
            "        }\n" +
            "\n" +
            "        response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
            "        response.setRemark(\"putMessageResult is null\");\n" +
            "        return response;\n" +
            "    }";

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
