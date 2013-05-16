package com.alibaba.rocketmq.namesrv.sync;

import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author lansheng.zj@taobao.com
 * 
 * @param <R>
 *            返回值类型
 * @param <T>
 *            参数类型
 */
public class TaskGroupExecutor<R, T> {
    private ExecutorService executorService;

    
    public TaskGroupExecutor(ThreadFactory threadFactory) {
        this(32, threadFactory);
    }
    

    public TaskGroupExecutor(int nThreads, ThreadFactory threadFactory) {
        executorService = Executors.newFixedThreadPool(nThreads, threadFactory);
    }


    public TaskGroupExecutor(int nThreads) {
        executorService = Executors.newFixedThreadPool(nThreads, new ThreadFactory() {

            private AtomicInteger index = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "taskgroup-executor-" + index.getAndIncrement());
            }
        });
    }


    public FutureGroup<R> groupCommit(TaskGroup<R, T> taskGroup, final T t) {
        final CountDownLatch latch = new CountDownLatch(taskGroup.size());
        FutureGroup<R> future = new FutureGroup<R>(latch, taskGroup.getKeys());

        for (final Entry<String, Task<R, T>> entry : taskGroup) {
            future.addResult(entry.getKey(), executorService.submit(new Callable<R>() {

                @Override
                public R call() throws Exception {

                    try {
                        return entry.getValue().exec(t);
                    }
                    catch (Exception e) {
                        // TODO

                        throw e;
                    }
                    finally {
                        latch.countDown();
                    }
                }

            }));
        }

        return future;
    }


    public void shutdown() {
        executorService.shutdown();
    }

}
