package org.apache.rocketmq.store;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * @author song
 */
public class StoreStatsServiceTest {

  @Test
  public void getSinglePutMessageTopicSizeTotal() throws Exception {
    final StoreStatsService storeStatsService = new StoreStatsService();
    int num = Runtime.getRuntime().availableProcessors() * 2;
    for (int j = 0; j < 100; j++) {
      final AtomicReference<AtomicLong> reference = new AtomicReference<>(null);
      final CountDownLatch latch = new CountDownLatch(num);
      final CyclicBarrier barrier = new CyclicBarrier(num);
      for (int i = 0; i < num; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
              AtomicLong atomicLong = storeStatsService.getSinglePutMessageTopicSizeTotal("test");
              if(reference.compareAndSet(null,atomicLong)){
              }else if (reference.get() != atomicLong){
                throw new RuntimeException("Reference should be same!");
              }
            } catch (InterruptedException | BrokenBarrierException e) {
              e.printStackTrace();
            }finally {
              latch.countDown();
            }
          }
        }).start();
      }
      latch.await();
    }
  }

  @Test
  public void getSinglePutMessageTopicTimesTotal() throws Exception {
    final StoreStatsService storeStatsService = new StoreStatsService();
    int num = Runtime.getRuntime().availableProcessors() * 2;
    for (int j = 0; j < 100; j++) {
      final AtomicReference<AtomicLong> reference = new AtomicReference<>(null);
      final CountDownLatch latch = new CountDownLatch(num);
      final CyclicBarrier barrier = new CyclicBarrier(num);
      for (int i = 0; i < num; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
              AtomicLong atomicLong = storeStatsService.getSinglePutMessageTopicTimesTotal("test");
              if(reference.compareAndSet(null,atomicLong)){
              }else if (reference.get() != atomicLong){
                throw new RuntimeException("Reference should be same!");
              }
            } catch (InterruptedException | BrokenBarrierException e) {
              e.printStackTrace();
            }finally {
              latch.countDown();
            }
          }
        }).start();
      }
      latch.await();
    }
  }

}