package com.alibaba.rocketmq.filtersrv.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;


public class FilterClassInfo {
    private String className;
    private int classCRC;
    private MessageFilter messageFilter;


    public int getClassCRC() {
        return classCRC;
    }


    public void setClassCRC(int classCRC) {
        this.classCRC = classCRC;
    }


    public MessageFilter getMessageFilter() {
        return messageFilter;
    }


    public void setMessageFilter(MessageFilter messageFilter) {
        this.messageFilter = messageFilter;
    }


    public String getClassName() {
        return className;
    }


    public void setClassName(String className) {
        this.className = className;
    }
}
