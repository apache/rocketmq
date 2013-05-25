package com.alibaba.rocketmq.research.udp;

import java.net.DatagramSocket;
import java.net.SocketException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class UDPTest {

    public static void main(String[] args) throws SocketException {
        DatagramSocket datagramSocket = new DatagramSocket(4500);
        System.out.println("OK 4500");

        new DatagramSocket(4500);
        System.out.println("OK 4500");
    }
}
