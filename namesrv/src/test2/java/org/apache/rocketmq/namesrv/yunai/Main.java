package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "public class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {\n" +
            "    private List<MessageQueue> messageQueueList;\n" +
            "\n" +
            "    @Override\n" +
            "    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,\n" +
            "        List<String> cidAll) {\n" +
            "        return this.messageQueueList;\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public String getName() {\n" +
            "        return \"CONFIG\";\n" +
            "    }\n" +
            "\n" +
            "    public List<MessageQueue> getMessageQueueList() {\n" +
            "        return messageQueueList;\n" +
            "    }\n" +
            "\n" +
            "    public void setMessageQueueList(List<MessageQueue> messageQueueList) {\n" +
            "        this.messageQueueList = messageQueueList;\n" +
            "    }\n" +
            "}\n"
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
