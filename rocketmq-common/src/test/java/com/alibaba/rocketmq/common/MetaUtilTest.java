/**
 * $Id: MetaUtilTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.Properties;

import org.junit.Test;


public class MetaUtilTest {

    class DemoConfig {
        private int demoWidth = 0;
        private int demoLength = 0;
        private boolean demoOK = false;
        private String demoName = "haha";


        public int getDemoWidth() {
            return demoWidth;
        }


        public void setDemoWidth(int demoWidth) {
            this.demoWidth = demoWidth;
        }


        public int getDemoLength() {
            return demoLength;
        }


        public void setDemoLength(int demoLength) {
            this.demoLength = demoLength;
        }


        public boolean isDemoOK() {
            return demoOK;
        }


        public void setDemoOK(boolean demoOK) {
            this.demoOK = demoOK;
        }


        public String getDemoName() {
            return demoName;
        }


        public void setDemoNfieldame(String demoName) {
            this.demoName = demoName;
        }
    }


    @Test
    public void test_a() {
        URL url = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        System.out.println(url);
        System.out.println(url.getPath());
    }


    @Test
    public void test_resetClassProperties() {
        DemoConfig demoConfig = new DemoConfig();
        MixAll.properties2Object(new Properties(), demoConfig);
    }


    @Test
    public void test_printClassProperties() {
        DemoConfig demoConfig = new DemoConfig();
    }


    @Test
    public void test_properties2String() {
        DemoConfig demoConfig = new DemoConfig();
        Properties properties = MixAll.object2Properties(demoConfig);
        System.out.println(MixAll.properties2String(properties));
    }


    @Test
    public void test_getTotalPhysicalMemorySize() {
        System.out.println(MixAll.TotalPhysicalMemorySize);
    }


    @Test
    public void test_timeMillisToHumanString() {
        System.out.println(MetaUtil.timeMillisToHumanString());
    }


    @Test
    public void test_isPropertiesEqual() {
        final Properties p1 = new Properties();
        final Properties p2 = new Properties();

        p1.setProperty("a", "1");
        p1.setProperty("b", "2");

        p2.setProperty("a", "1");
        p2.setProperty("b", "2");
        //p2.setProperty("c", "3");

        assertTrue(MixAll.isPropertiesEqual(p1, p2));
    }


    @Test
    public void test_getpid() {
        int pid = MetaUtil.getPid();

        System.out.println("PID = " + pid);
        assertTrue(pid > 0);
    }
}
