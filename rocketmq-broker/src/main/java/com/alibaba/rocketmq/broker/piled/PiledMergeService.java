package com.alibaba.rocketmq.broker.piled;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.queue.ConcurrentTreeMap;
import com.alibaba.rocketmq.store.MapedFile;


/**
 * @auther lansheng.zj
 */
public class PiledMergeService extends ServiceThread {

    public static final int SKIP_BYTE = 63;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private ConcurrentTreeMap<MapedFile, CountDownLatch> tree;
    private BrokerController brokerController;


    public PiledMergeService(BrokerController brokerController) {
        this.brokerController = brokerController;
        Comparator<MapedFile> comparator = new Comparator<MapedFile>() {

            @Override
            public int compare(MapedFile o1, MapedFile o2) {
                int index1 = o1.getFileName().lastIndexOf(File.separator);
                long name1 = Long.parseLong(o1.getFileName().substring(index1 + 1));
                int index2 = o2.getFileName().lastIndexOf(File.separator);
                long name2 = Long.parseLong(o2.getFileName().substring(index2 + 1));

                if (name1 < name2) {
                    return -1;
                }
                else if (name1 > name2) {
                    return 2;
                }
                else {
                    return 0;
                }
            }
        };

        tree =
                new ConcurrentTreeMap<MapedFile, CountDownLatch>(this.brokerController.getBrokerConfig()
                    .getPiledElementAmount(), comparator);
    }


    public CountDownLatch put(MapedFile key) {
        CountDownLatch tmp = tree.putIfAbsentAndRetExsit(key, new CountDownLatch(1));
        if (null != tmp) {
            // notify
            wakeup();
        }
        else {
            tmp = new CountDownLatch(0);
        }

        return tmp;
    }


    @Override
    public void run() {
        log.warn(this.getServiceName() + " service started");
        while (!this.isStoped()) {
            try {
                this.waitForRunning(0);
                warmup();
            }
            catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }

        }
        log.warn(this.getServiceName() + " service end");
    }


    public void warmup() {
        Entry<MapedFile, CountDownLatch> entry = null;
        while (null != (entry = tree.pollFirstEntry())) {
            MapedFile mapedFile = entry.getKey();
            CountDownLatch coutDown = entry.getValue();
            long start = System.currentTimeMillis();

            if (brokerController.getBrokerConfig().getWarmType() == 0) {
                mapedFile.getMappedByteBuffer().load();
            }
            else if (brokerController.getBrokerConfig().getWarmType() == 1) {
                doWarmup(mapedFile);
            }

            coutDown.countDown();
            long cost = System.currentTimeMillis() - start;
            log.warn("warm up {} cost {} countdown count {}", mapedFile.getFileName(), cost,
                coutDown.getCount());
        }
    }


    public void doWarmup(MapedFile mapedFile) {
        ByteBuffer buffer = mapedFile.getMappedByteBuffer().slice();
        while (buffer.hasRemaining()) {
            buffer.get();
            if (buffer.position() + SKIP_BYTE >= buffer.limit()) {
                break;
            }
            else {
                buffer.position(buffer.position() + SKIP_BYTE);
            }
        }
    }


    @Override
    public String getServiceName() {
        return PiledMergeService.class.getSimpleName();
    }

}
