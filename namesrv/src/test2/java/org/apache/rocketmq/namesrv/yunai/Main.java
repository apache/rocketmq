package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR =
            "// 【MySQLConnectionHandler.java】\n" +
                    "\t@Override\n" +
                    "\tprotected void handleData(byte[] data) {\n" +
                    "\t\tswitch (resultStatus) {\n" +
                    "\t\tcase RESULT_STATUS_INIT:\n" +
                    "\t\t\tswitch (data[4]) {\n" +
                    "\t\t\tcase OkPacket.FIELD_COUNT:\n" +
                    "\t\t\t\thandleOkPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tcase ErrorPacket.FIELD_COUNT:\n" +
                    "\t\t\t\thandleErrorPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tcase RequestFilePacket.FIELD_COUNT:\n" +
                    "\t\t\t\thandleRequestPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tdefault: // 初始化 header fields\n" +
                    "\t\t\t\tresultStatus = RESULT_STATUS_HEADER;\n" +
                    "\t\t\t\theader = data;\n" +
                    "\t\t\t\tfields = new ArrayList<byte[]>((int) ByteUtil.readLength(data,\n" +
                    "\t\t\t\t\t\t4));\n" +
                    "\t\t\t}\n" +
                    "\t\t\tbreak;\n" +
                    "\t\tcase RESULT_STATUS_HEADER:\n" +
                    "\t\t\tswitch (data[4]) {\n" +
                    "\t\t\tcase ErrorPacket.FIELD_COUNT:\n" +
                    "\t\t\t\tresultStatus = RESULT_STATUS_INIT;\n" +
                    "\t\t\t\thandleErrorPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tcase EOFPacket.FIELD_COUNT: // 解析 fields 结束\n" +
                    "\t\t\t\tresultStatus = RESULT_STATUS_FIELD_EOF;\n" +
                    "\t\t\t\thandleFieldEofPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tdefault: // 解析 fields\n" +
                    "\t\t\t\tfields.add(data);\n" +
                    "\t\t\t}\n" +
                    "\t\t\tbreak;\n" +
                    "\t\tcase RESULT_STATUS_FIELD_EOF:\n" +
                    "\t\t\tswitch (data[4]) {\n" +
                    "\t\t\tcase ErrorPacket.FIELD_COUNT:\n" +
                    "\t\t\t\tresultStatus = RESULT_STATUS_INIT;\n" +
                    "\t\t\t\thandleErrorPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tcase EOFPacket.FIELD_COUNT: // 解析 每行记录 结束\n" +
                    "\t\t\t\tresultStatus = RESULT_STATUS_INIT;\n" +
                    "\t\t\t\thandleRowEofPacket(data);\n" +
                    "\t\t\t\tbreak;\n" +
                    "\t\t\tdefault: // 每行记录\n" +
                    "\t\t\t\thandleRowPacket(data);\n" +
                    "\t\t\t}\n" +
                    "\t\t\tbreak;\n" +
                    "\t\tdefault:\n" +
                    "\t\t\tthrow new RuntimeException(\"unknown status!\");\n" +
                    "\t\t}\n" +
                    "\t}" + "\n"
            ;

    public static void main(String[] args) {
        int i = 1;
        boolean replaceBlank = STR.split("\n")[0].contains("class")
                || STR.split("\n")[0].contains("interface");
        for (String str : STR.split("\n")) {
            if (!replaceBlank) {
                str = str.replaceFirst("    ", "");
                str = str.replaceFirst("\t", "");
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
