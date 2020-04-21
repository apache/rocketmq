package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.common.UtilAll;

/**
 * @author Dovelol
 * @date 2019/11/14 22:27
 */
public class Demo {

    public static void main(String[] args) {
        byte[] bytes = UtilAll.string2bytes("C0A89D0100002A9F0000000000000A72");
        byte[] b = new byte[4];
        b[0] = bytes[0];
        b[1] = bytes[1];
        b[2] = bytes[2];
        b[3] = bytes[3];
        System.out.println(new String(b));
    }

}
