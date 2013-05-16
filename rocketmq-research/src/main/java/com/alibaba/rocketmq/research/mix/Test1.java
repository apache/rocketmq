/**
 * $Id: Test1.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.mix;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class Test1 {
    private int a = 10;


    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        // String processName =
        // java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        //
        // String processID = processName.substring(0,
        // processName.indexOf('@'));
        //
        // System.out.println("processID=" + processID);
        
        
//        int i = 0;
//        
//        Integer abc = 99;
//        i = abc;
//        System.out.println(abc);
        
        long i = 100L;
        Object obj = i;
        System.out.println(obj);
        System.out.println(obj.getClass());

        Test1 test1 = Test1.class.newInstance();

        test1.register(Test1.class);
        System.out.println(test1);
    }


    public void register(Class<? extends Object> t) {

    }


    @Override
    public String toString() {
        return "Test1 [a=" + a + "]";
    }

}
