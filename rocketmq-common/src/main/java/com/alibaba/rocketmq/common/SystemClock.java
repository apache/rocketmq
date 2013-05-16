/**
 * $Id: SystemClock.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 后台定时更新时钟，JVM退出时，线程自动回收
 * 
 * @see 
 *      <A>https://github.com/zhongl/jtoolkit/blob/master/common/src/main/java/com
 *      /github/zhongl/jtoolkit/SystemClock.java</A>
 * @author vintage.wang@gmail.com  shijia.wxr@taobao.com
 */
public class SystemClock {

    private final long precision;
    private final AtomicLong now;


    public SystemClock(long precision) {
        this.precision = precision;
        now = new AtomicLong(System.currentTimeMillis());
        scheduleClockUpdating();
    }


    private void scheduleClockUpdating() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "System Clock");
                thread.setDaemon(true);
                return thread;
            }
        });
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                now.set(System.currentTimeMillis());
            }
        }, precision, precision, TimeUnit.MILLISECONDS);
    }


    public long now() {
        return now.get();
    }


    public long precision() {
        return precision;
    }
}
