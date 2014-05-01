package com.alibaba.rocketmq.broker.filtersrv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;


public class FilterServerUtil {

    public static void callShell(final String shellString, final Logger log) {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(shellString);
            final InputStream is1 = process.getInputStream();
            new Thread(new Runnable() {
                public void run() {
                    BufferedReader br = new BufferedReader(new InputStreamReader(is1));
                    try {
                        while (br.readLine() != null)
                            ;
                    }
                    catch (IOException e) {
                        log.error("callShell: readLine IOException, " + shellString, e);
                    }
                }
            }).start(); // 启动单独的线程来清空process.getInputStream()的缓冲区

            InputStream is2 = process.getErrorStream();
            BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
            StringBuilder buf = new StringBuilder(); // 保存输出结果流
            String line = null;
            while ((line = br2.readLine()) != null)
                buf.append(line); // 循环等待ffmpeg进程结束

            log.info("callShell: <{}> OK", shellString);
        }
        catch (Throwable e) {
            log.error("callShell: readLine IOException, " + shellString, e);
        }
        finally {
            if (null != process)
                process.destroy();
        }
    }
}
