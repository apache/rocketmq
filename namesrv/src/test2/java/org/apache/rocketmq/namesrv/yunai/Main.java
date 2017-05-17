package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR =
            "// 【DefaultRequestProcessor.java】\n" +
                    "    /**\n" +
                    "     * 拉取消息\n" +
                    "     *\n" +
                    "     * @param ctx 拉取消息context\n" +
                    "     * @param request 拉取消息请求\n" +
                    "     * @return 响应\n" +
                    "     * @throws Exception 当发生异常时\n" +
                    "     */\n" +
                    "    private RemotingCommand pullMessageForward(final ChannelHandlerContext ctx, final RemotingCommand request) throws Exception {\n" +
                    "        final RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);\n" +
                    "        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();\n" +
                    "        final PullMessageRequestHeader requestHeader =\n" +
                    "            (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);\n" +
                    "\n" +
                    "        final FilterContext filterContext = new FilterContext();\n" +
                    "        filterContext.setConsumerGroup(requestHeader.getConsumerGroup());\n" +
                    "\n" +
                    "        response.setOpaque(request.getOpaque());\n" +
                    "\n" +
                    "        DefaultMQPullConsumer pullConsumer = this.filtersrvController.getDefaultMQPullConsumer();\n" +
                    "\n" +
                    "        // 校验Topic过滤类是否完整\n" +
                    "        final FilterClassInfo findFilterClass = this.filtersrvController.getFilterClassManager().findFilterClass(requestHeader.getConsumerGroup(), requestHeader.getTopic());\n" +
                    "        if (null == findFilterClass) {\n" +
                    "            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
                    "            response.setRemark(\"Find Filter class failed, not registered\");\n" +
                    "            return response;\n" +
                    "        }\n" +
                    "        if (null == findFilterClass.getMessageFilter()) {\n" +
                    "            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
                    "            response.setRemark(\"Find Filter class failed, registered but no class\");\n" +
                    "            return response;\n" +
                    "        }\n" +
                    "\n" +
                    "        // 设置下次请求从 Broker主节点。\n" +
                    "        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);\n" +
                    "\n" +
                    "        MessageQueue mq = new MessageQueue();\n" +
                    "        mq.setTopic(requestHeader.getTopic());\n" +
                    "        mq.setQueueId(requestHeader.getQueueId());\n" +
                    "        mq.setBrokerName(this.filtersrvController.getBrokerName());\n" +
                    "        long offset = requestHeader.getQueueOffset();\n" +
                    "        int maxNums = requestHeader.getMaxMsgNums();\n" +
                    "\n" +
                    "        final PullCallback pullCallback = new PullCallback() {\n" +
                    "\n" +
                    "            @Override\n" +
                    "            public void onSuccess(PullResult pullResult) {\n" +
                    "                responseHeader.setMaxOffset(pullResult.getMaxOffset());\n" +
                    "                responseHeader.setMinOffset(pullResult.getMinOffset());\n" +
                    "                responseHeader.setNextBeginOffset(pullResult.getNextBeginOffset());\n" +
                    "                response.setRemark(null);\n" +
                    "\n" +
                    "                switch (pullResult.getPullStatus()) {\n" +
                    "                    case FOUND:\n" +
                    "                        response.setCode(ResponseCode.SUCCESS);\n" +
                    "\n" +
                    "                        List<MessageExt> msgListOK = new ArrayList<MessageExt>();\n" +
                    "                        try {\n" +
                    "                            for (MessageExt msg : pullResult.getMsgFoundList()) {\n" +
                    "                                // 使用过滤类过滤消息\n" +
                    "                                boolean match = findFilterClass.getMessageFilter().match(msg, filterContext);\n" +
                    "                                if (match) {\n" +
                    "                                    msgListOK.add(msg);\n" +
                    "                                }\n" +
                    "                            }\n" +
                    "\n" +
                    "                            if (!msgListOK.isEmpty()) {\n" +
                    "                                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, response, msgListOK);\n" +
                    "                                return;\n" +
                    "                            } else {\n" +
                    "                                response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);\n" +
                    "                            }\n" +
                    "                        } catch (Throwable e) {\n" +
                    "                            final String error =\n" +
                    "                                String.format(\"do Message Filter Exception, ConsumerGroup: %s Topic: %s \",\n" +
                    "                                    requestHeader.getConsumerGroup(), requestHeader.getTopic());\n" +
                    "                            log.error(error, e);\n" +
                    "\n" +
                    "                            response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
                    "                            response.setRemark(error + RemotingHelper.exceptionSimpleDesc(e));\n" +
                    "                            returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, response, null);\n" +
                    "                            return;\n" +
                    "                        }\n" +
                    "\n" +
                    "                        break;\n" +
                    "                    case NO_MATCHED_MSG:\n" +
                    "                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);\n" +
                    "                        break;\n" +
                    "                    case NO_NEW_MSG:\n" +
                    "                        response.setCode(ResponseCode.PULL_NOT_FOUND);\n" +
                    "                        break;\n" +
                    "                    case OFFSET_ILLEGAL:\n" +
                    "                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);\n" +
                    "                        break;\n" +
                    "                    default:\n" +
                    "                        break;\n" +
                    "                }\n" +
                    "\n" +
                    "                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, response, null);\n" +
                    "            }\n" +
                    "\n" +
                    "            @Override\n" +
                    "            public void onException(Throwable e) {\n" +
                    "                response.setCode(ResponseCode.SYSTEM_ERROR);\n" +
                    "                response.setRemark(\"Pull Callback Exception, \" + RemotingHelper.exceptionSimpleDesc(e));\n" +
                    "                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, response, null);\n" +
                    "                return;\n" +
                    "            }\n" +
                    "        };\n" +
                    "\n" +
                    "        // 拉取消息\n" +
                    "        pullConsumer.pullBlockIfNotFound(mq, null, offset, maxNums, pullCallback);\n" +
                    "        return null;\n" +
                    "    }"
            ;

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
