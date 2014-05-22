package com.alibaba.rocketmq.filtersrv.filter;

public class FilterClassLoader extends ClassLoader {
    public final Class<?> createNewClass(String name, byte[] b, int off, int len) throws ClassFormatError {
        return this.defineClass(name, b, off, len);
    }
}
