package org.apache.rocketmq.broker.transaction;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Assert;
import org.junit.Test;

public class AbstractTransactionalMessageCheckListenerTest {

    @Test
    public void testResolveHalfMsg() throws Exception {
        AtomicInteger execCount = new AtomicInteger(0);
        AbstractTransactionalMessageCheckListener listener = new AbstractTransactionalMessageCheckListener() {
            @Override
            public void resolveDiscardMsg(MessageExt msgExt) {
            }

            @Override
            public void sendCheckMessage(MessageExt msgExt) throws Exception {
                Thread.sleep(10);
                execCount.incrementAndGet();
            }
        };
        ExecutorService old = AbstractTransactionalMessageCheckListener.executorService;
        try {
            MessageExt messageExt = new MessageExt();
            ExecutorService es = new ThreadPoolExecutor(1, 1, 100,
                    TimeUnit.SECONDS, new ArrayBlockingQueue<>(5),
                    r -> {
                        Thread thread = new Thread(r);
                        thread.setName("Transaction-msg-check-thread");
                        return thread;
                    });
            AbstractTransactionalMessageCheckListener.executorService = es;
            int count = 10;
            for (int i = 0; i < count; i++) {
                listener.resolveHalfMsg(messageExt);
            }
            listener.shutDown();
            listener.resolveHalfMsg(messageExt);
            es.awaitTermination(5, TimeUnit.SECONDS);
            Assert.assertEquals(count, execCount.get());
        } finally {
            AbstractTransactionalMessageCheckListener.executorService = old;
        }
    }
}
