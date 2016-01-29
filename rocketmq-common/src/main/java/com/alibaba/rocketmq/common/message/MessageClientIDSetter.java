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

import io.netty.util.internal.StringUtil;

/**
 * 类MessageClientIDDecoder.java的实现描述：TODO 类实现描述 
 * @author yp 2016年1月26日 下午5:53:01
 */
public class MessageClientIDSetter {
                
    private static int counter;
    
    private static boolean validate;
    
    private static int basePos = 0;
    
    private static long startTime;
    
    //ip  + pid + classloaderid + counter + time
    //4 bytes for ip , 4 bytes for pid, 4 bytes for  classloaderid
    //4 bytes for counter,  4 bytes for timediff, 
    private static StringBuilder sb = null;
    
    private static ByteBuffer buffer = ByteBuffer.allocate(4 + 4); 
    
    static {

        int len = 4 + 4 + 4  + 4  + 4;        
        try {
            //分配空间
            sb = new StringBuilder(len*2);
            ByteBuffer tempBuffer =  ByteBuffer.allocate(len - buffer.limit());                
            //本机ip, 进程id，classloader标识
            tempBuffer.put(UtilAll.getIP());            
            tempBuffer.putInt(UtilAll.getPid());
            tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
            sb.append(UtilAll.bytes2string(tempBuffer.array()));            
            basePos = sb.length();
            //以2016 1 1 为基准计算差值，一个int，可以存储n年，足够用了
            Calendar cal = Calendar.getInstance();
            cal.set(2016, 1, 1);
            startTime = cal.getTimeInMillis();
            //计数器
            counter = 0;
            validate = true;
        }
        catch (Exception e) {
            validate = false;
            System.out.println("MessageClientIDSetter initialize error");
            e.printStackTrace();            
        }
    }
    
    private static synchronized String createUniqID() {
        if (validate) {
            //连接正常唯一id
            buffer.position(0);          
            sb.setLength(basePos);            
            buffer.putInt(counter++);           
            buffer.putInt((int)((System.currentTimeMillis() - startTime)/1000));
            sb.append(UtilAll.bytes2string(buffer.array()));
            return sb.toString();
        }
        else {
            //如果ip无效，则生成一个UUID
            return UUID.randomUUID().toString();
        }
    }
    
    /**
     * 这个方法会在property中存储uniqkey在keys中的index,一旦后续这个index改变，程序会出问题，
     * 因此后续只能向keys后面加入key，不能往前面插入key
     * @param msg
     */
    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            String uniqID = createUniqID();
            msg.appendKey(uniqID);
            int keysIdx = msg.getKeys().split(MessageConst.KEY_SEPARATOR).length - 1;
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, String.valueOf(keysIdx));            
        }
    }
        
    public static void main(String[] args) {
            
        System.out.println(Short.MAX_VALUE);
        System.out.println(Byte.MAX_VALUE);
        
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            UUID.randomUUID().toString();
        }
        long end =  System.currentTimeMillis();
        System.out.println(end - begin); //3812
        
        begin = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            createUniqID();
        }
        end =  System.currentTimeMillis();
        System.out.println(end - begin);//783
        
        long testlong = (long)Integer.MAX_VALUE + 1L;
        System.out.println(testlong);
        
        System.out.println(UUID.randomUUID().toString());
        System.out.println(createUniqID());
        
        for (int i = 0; i < 20; i++) {
            Message test = new Message();
            MessageClientIDSetter.setUniqID(test);
            System.out.println(test.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        }
        
        System.out.println("end");
        
    }
}
    
