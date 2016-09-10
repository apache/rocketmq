/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.common.message;

import com.alibaba.rocketmq.common.UtilAll;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

public class MessageClientIDSetter {
                
    private static short counter;
    
    private static int basePos = 0;
    
    private static long startTime;
    
    private static long nextStartTime;
    
    //ip  + pid + classloaderid + counter + time
    //4 bytes for ip , 4 bytes for pid, 4 bytes for  classloaderid
    //2 bytes for counter,  4 bytes for timediff, 
    private static StringBuilder sb = null;
    
    private static ByteBuffer buffer = ByteBuffer.allocate(4 + 2); 
    
    private static final String TOPIC_KEY_SPLITTER = "#";
    
    static {
        int len = 4 + 2 + 4  + 4 + 2;        
        

        sb = new StringBuilder(len*2);
        ByteBuffer tempBuffer =  ByteBuffer.allocate(len - buffer.limit());                

        tempBuffer.position(2);
        tempBuffer.putInt(UtilAll.getPid());//2
        tempBuffer.position(0);
        try {
            tempBuffer.put(UtilAll.getIP());    //4
        }
        catch (Exception e) {
            tempBuffer.put(createFakeIP()); //4
        }
        tempBuffer.position(6);
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode()); //4
        sb.append(UtilAll.bytes2string(tempBuffer.array()));            
        basePos = sb.length();

        setStartTime(System.currentTimeMillis());

        counter = 0;

    }
    
    private static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH,1);
        cal.set(Calendar.HOUR,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);       
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }
    
    public static Date getNearlyTimeFromID(String msgID) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        byte[] bytes = UtilAll.string2bytes(msgID);
        buf.put((byte) 0);buf.put((byte) 0);buf.put((byte) 0);buf.put((byte) 0);
        buf.put(bytes, 10, 4);
        buf.position(0);
        long spanMS = buf.getLong();
        
        Calendar cal = Calendar.getInstance();
        long now = cal.getTimeInMillis();
        cal.set(Calendar.DAY_OF_MONTH,1);
        cal.set(Calendar.HOUR,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);        
        cal.set(Calendar.MILLISECOND,0);
        long monStartTime = cal.getTimeInMillis();
        if (monStartTime + spanMS >= now) {
            cal.add(Calendar.MONTH, -1);
            monStartTime = cal.getTimeInMillis();
        }
        cal.setTimeInMillis(monStartTime + spanMS);
        return cal.getTime();
    }
    
    public static String getIPStrFromID(String msgID) {
        byte[] ipBytes = getIPFromID(msgID);
        return UtilAll.ipToIPv4Str(ipBytes);
    }
    
    public static byte[] getIPFromID(String msgID) {  
        byte[] result = new byte[4];
        byte[] bytes = UtilAll.string2bytes(msgID);
        System.arraycopy(bytes, 0, result, 0, 4);
        return result;
    }
    
    public static synchronized String createUniqID() {
            long current = System.currentTimeMillis();
            if (current >= nextStartTime) {
                setStartTime(current);
            }            
            buffer.position(0);          
            sb.setLength(basePos);            
            buffer.putInt( (int)(System.currentTimeMillis() - startTime) );
            buffer.putShort(counter++);
            sb.append(UtilAll.bytes2string(buffer.array()));
            return sb.toString();
    }
    
    public static void setUniqID(final Message msg) {


        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());            
        }
    }
    
    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }
        
    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];        
        bb.get(fakeIP);
        return fakeIP;
    }
    
    private static void printIP(byte[] ip) {
        String str = UtilAll.ipToIPv4Str(ip);
        System.out.println(str);
    }
    
    public static void main(String[] args) throws Exception {
                         
        byte[] realIP = UtilAll.getIP();
        printIP(realIP);
        
        if (realIP[3] > (byte)248 && realIP[3] < (byte)250) {
            System.out.println("true");
        }
        
        byte[] fakeIP = createFakeIP();          
        printIP(fakeIP);
        
        byte[] innerIP = new byte[4];
        innerIP[0] = (byte)172;
        innerIP[1] = (byte)16;
        innerIP[2] = (byte)217;
        innerIP[3] = (byte)1;
        System.out.println("inner = " + UtilAll.isInternalIP(innerIP));
        
        System.out.println("end");
        
        
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(1457442383176L);
        System.out.println(cal.getTime());
        cal.setTimeInMillis(1457442382996L);
        System.out.println(cal.getTime());
        
        setStartTime(System.currentTimeMillis());
        
        Thread.currentThread().sleep(100);
        
        setStartTime(System.currentTimeMillis());
        
         long threeday = 1000*60*60*24*3;
        System.out.println(Long.toBinaryString(threeday));
        
        int d = Integer.parseInt("1111111111111111", 2);
        System.out.println(d);
        
        System.out.println(Short.MAX_VALUE);
        System.out.println(Byte.MAX_VALUE);
        
        String id = createUniqID();
        System.out.println(id);
        
        String ip = getIPStrFromID(id);
        
        Date date = getNearlyTimeFromID(id);
        
        System.out.println(ip);
        System.out.println(date.toString());
        
        System.out.println("end...");
        

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            UUID.randomUUID().toString();
        }
        long end =  System.currentTimeMillis();
        System.out.println(end - begin); //3576
        
        begin = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            createUniqID();
        }
        end =  System.currentTimeMillis();
        System.out.println(end - begin);//993
        

        HashSet<String> set = new HashSet<String>();
        for (int i = 0; i < 10000; i++) {
            set.add(createUniqID());
        }
        System.out.println(set.size());
        
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
    
