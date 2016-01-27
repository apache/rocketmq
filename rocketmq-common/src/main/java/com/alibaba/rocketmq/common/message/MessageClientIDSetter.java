/*
 * Copyright 2016 Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com .
 */
package com.alibaba.rocketmq.common.message;

import java.io.File;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.common.UtilAll;

/**
 * 类MessageClientIDDecoder.java的实现描述：TODO 类实现描述 
 * @author yp 2016年1月26日 下午5:53:01
 */
public class MessageClientIDSetter {
                
    private static AtomicInteger counter;
    
    private static boolean validate;
    
    private static int basePos = 0;
    
    //ip  + pid + classloaderid + counter + time
    //4 bytes for ip , 4 bytes for pid, 4 bytes for  classloaderid
    //4 bytes for counter,  8 bytes for time, 
    private static StringBuilder sb = null;
    
    static {
        int len = 4 + 4 + 4  + 4  + 8;        
        try {
            //分配空间
            sb = new StringBuilder(len*2);
            ByteBuffer tempBuffer =  ByteBuffer.allocate(4 + 4 + 4);                
            //本机ip, 进程id，classloader标识
            tempBuffer.put(UtilAll.getIP());            
            tempBuffer.putInt(UtilAll.getPid());
            tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
            sb.append(UtilAll.bytes2string(tempBuffer.array()));            
            basePos = sb.length();
                        
            //计数器
            counter = new AtomicInteger(0);
            validate = true;
        }
        catch (Exception e) {
            validate = false;
            System.out.println("MessageClientIDSetter initialize error");
            e.printStackTrace();            
        }
    }
    
    private static String createUniqID() {
        if (validate) {
            //连接正常唯一id
            ByteBuffer buffer = ByteBuffer.allocate(4 + 8);            
            sb.setLength(basePos);            
            buffer.putInt(counter.incrementAndGet());
            buffer.putLong(System.currentTimeMillis());
            sb.append(UtilAll.bytes2string(buffer.array()));
            return sb.toString();
        }
        else {
            //如果ip无效，则生成一个UUID
            return UUID.randomUUID().toString();
        }
    }
    
    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID) == null) {
            String uniqID = createUniqID();
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID, uniqID);
            msg.appendKey(uniqID);
        }
    }
        
    public static void main(String[] args) {
                
        Calendar cal = Calendar.getInstance();
        cal.set(3000, 1, 1);
        System.out.println(cal.getTimeInMillis());
        
        
        for (int i = 0; i < 20; i++) {
            Message test = new Message();
            MessageClientIDSetter.setUniqID(test);
            System.out.println(test.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID));
        }
        
        System.out.println("end");
        
    }
}
    
