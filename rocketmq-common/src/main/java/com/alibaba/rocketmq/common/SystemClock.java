/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * @see 
 *      <A>https://github.com/zhongl/jtoolkit/blob/master/common/src/main/java/com
 *      /github/zhongl/jtoolkit/SystemClock.java</A>
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
