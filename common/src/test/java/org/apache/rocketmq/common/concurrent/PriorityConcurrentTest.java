package org.apache.rocketmq.common.concurrent;

import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityConcurrentTest {

    @Test
    public void testPriorityConcurrent() throws Exception {
        PriorityConcurrentEngine.startAutoConsumer();
        PriorityConcurrentEngine.runPriorityAsync(() -> System.out.println("hello"));
        AtomicInteger count = new AtomicInteger(0);
        List<Integer> list = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 50; i++) {
            int priority = i;
            for (int j = 0; j < i; j++) {
                PriorityConcurrentEngine.supplyPriorityAsync(priority, new CallableSupplier<Integer>() {
                    @Override
                    public Integer get() {
                        try {
                            Thread.sleep(new Random().nextInt(100));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println(priority);
                        return priority;
                    }

                    @Override
                    public Callback<Integer> getCallback() {
                        return list::add;
                    }
                });
                count.getAndIncrement();
            }
        }
        Thread.sleep(5000);
        PriorityConcurrentEngine.runPriorityAsync(() -> {
            List<Integer> copy = new CopyOnWriteArrayList<>(list);
            copy.sort(Integer::compareTo);
            System.out.println(copy.equals(list));
            System.out.println(list.size());
            System.out.println(count.get());
            System.out.println(list);
        });
        PriorityConcurrentEngine.shutdown();
    }
}
