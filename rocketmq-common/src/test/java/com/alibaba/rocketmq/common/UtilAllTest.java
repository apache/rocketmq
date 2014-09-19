package com.alibaba.rocketmq.common;

import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Properties;

import org.junit.Test;


public class UtilAllTest {

    @Test
    public void test_currentStackTrace() {
        System.out.println(UtilAll.currentStackTrace());
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
    public void test_properties2String() {
        DemoConfig demoConfig = new DemoConfig();
        Properties properties = MixAll.object2Properties(demoConfig);
        System.out.println(MixAll.properties2String(properties));
    }


    @Test
    public void test_timeMillisToHumanString() {
        System.out.println(UtilAll.timeMillisToHumanString());
    }


    @Test
    public void test_isPropertiesEqual() {
        final Properties p1 = new Properties();
        final Properties p2 = new Properties();

        p1.setProperty("a", "1");
        p1.setProperty("b", "2");

        p2.setProperty("a", "1");
        p2.setProperty("b", "2");
        // p2.setProperty("c", "3");

        assertTrue(MixAll.isPropertiesEqual(p1, p2));
    }


    @Test
    public void test_getpid() {
        int pid = UtilAll.getPid();

        System.out.println("PID = " + pid);
        assertTrue(pid > 0);
    }


    @Test
    public void test_isBlank() {
        {
            boolean result = UtilAll.isBlank("Hello ");
            assertTrue(!result);
        }

        {
            boolean result = UtilAll.isBlank(" Hello");
            assertTrue(!result);
        }

        {
            boolean result = UtilAll.isBlank("He llo");
            assertTrue(!result);
        }

        {
            boolean result = UtilAll.isBlank("  ");
            assertTrue(result);
        }

        {
            boolean result = UtilAll.isBlank("Hello");
            assertTrue(!result);
        }
    }

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
}
