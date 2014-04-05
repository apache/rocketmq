package com.alibaba.rocketmq.research.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import com.alibaba.rocketmq.common.UtilAll;


class MyClassLoader extends ClassLoader {
    public final Class<?> createNewClass(String name, byte[] b, int off, int len) throws ClassFormatError {
        return this.defineClass(name, b, off, len);
    }
}


public class Test {
    public void hello() {
        System.out.println("Hello, I am loaded from binary");
    }


    public static void main(String[] args) throws InstantiationException, IllegalAccessException, IOException {
        String name = MessageFilterImpl.class.getName().replace(".", "/") + ".class";
        System.out.println(name);

        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(name);
        int ava = is.available();
        byte[] content = new byte[ava];

        int crc32 = UtilAll.crc32(content);
        System.out.println("crc=" + crc32);

        System.out.println("ava = " + ava);
        int len = is.read(content);

        // 打开一个随机访问文件流，按读写方式
        RandomAccessFile randomFile = new RandomAccessFile("/Users/vive/c.txt", "rw");
        randomFile.seek(0);
        randomFile.write(content, 0, len);
        randomFile.close();
        System.out.println(len);

        MyClassLoader my = new MyClassLoader();
        Class<?> newClass = my.createNewClass(null, content, 0, content.length);
        Object newInstance = newClass.newInstance();
        System.out.println(newInstance);

        MessageFilter filter = (MessageFilter) newInstance;
        filter.doFilter(null);
    }


    @Override
    public String toString() {
        return "Test [toString()=" + super.toString() + "]";
    }
}
