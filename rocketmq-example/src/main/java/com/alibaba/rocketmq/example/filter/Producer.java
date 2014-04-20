package com.alibaba.rocketmq.example.filter;

import java.net.URL;


public class Producer {

    public static void main(String[] args) {
        String className = "rocketmq.message.filter.cousumergroup1.FilterTest";
        final String javaSource = className + ".java";
        URL url = Producer.class.getClassLoader().getResource(javaSource);
        System.out.println(url.getFile());
    }
}
