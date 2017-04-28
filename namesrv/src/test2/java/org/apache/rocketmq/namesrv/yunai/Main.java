package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "public class ConsumerOffsetManager extends ConfigManager {\n" +
            "    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);\n" +
            "    private static final String TOPIC_GROUP_SEPARATOR = \"@\";\n" +
            "\n" +
            "    /**\n" +
            "     * 消费进度集合\n" +
            "     */\n" +
            "    private ConcurrentHashMap<String/* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);\n" +
            "\n" +
            "    private transient BrokerController brokerController;\n" +
            "\n" +
            "    public ConsumerOffsetManager() {\n" +
            "    }\n" +
            "\n" +
            "    public ConsumerOffsetManager(BrokerController brokerController) {\n" +
            "        this.brokerController = brokerController;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 提交消费进度\n" +
            "     *\n" +
            "     * @param clientHost 提交client地址\n" +
            "     * @param group 消费分组\n" +
            "     * @param topic 主题\n" +
            "     * @param queueId 队列编号\n" +
            "     * @param offset 进度（队列位置）\n" +
            "     */\n" +
            "    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId, final long offset) {\n" +
            "        // topic@group\n" +
            "        String key = topic + TOPIC_GROUP_SEPARATOR + group;\n" +
            "        this.commitOffset(clientHost, key, queueId, offset);\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 提交消费进度\n" +
            "     *\n" +
            "     * @param clientHost 提交client地址\n" +
            "     * @param key 主题@消费分组\n" +
            "     * @param queueId 队列编号\n" +
            "     * @param offset 进度（队列位置）\n" +
            "     */\n" +
            "    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {\n" +
            "        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);\n" +
            "        if (null == map) {\n" +
            "            map = new ConcurrentHashMap<>(32);\n" +
            "            map.put(queueId, offset);\n" +
            "            this.offsetTable.put(key, map);\n" +
            "        } else {\n" +
            "            Long storeOffset = map.put(queueId, offset);\n" +
            "            if (storeOffset != null && offset < storeOffset) {\n" +
            "                log.warn(\"[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}\", clientHost, key, queueId, offset, storeOffset);\n" +
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    public String encode() {\n" +
            "        return this.encode(false);\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public String configFilePath() {\n" +
            "        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 解码内容\n" +
            "     * 格式:JSON\n" +
            "     *\n" +
            "     * @param jsonString 内容\n" +
            "     */\n" +
            "    @Override\n" +
            "    public void decode(String jsonString) {\n" +
            "        if (jsonString != null) {\n" +
            "            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);\n" +
            "            if (obj != null) {\n" +
            "                this.offsetTable = obj.offsetTable;\n" +
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 编码内容\n" +
            "     * 格式为JSON\n" +
            "     *\n" +
            "     * @param prettyFormat 是否格式化\n" +
            "     * @return 编码后的内容\n" +
            "     */\n" +
            "    public String encode(final boolean prettyFormat) {\n" +
            "        return RemotingSerializable.toJson(this, prettyFormat);\n" +
            "    }\n" +
            "\n" +
            "}\n";

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
