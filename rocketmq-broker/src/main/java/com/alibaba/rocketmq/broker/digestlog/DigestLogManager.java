package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.rocketmq.broker.BrokerController;


/**
 * 统计管理器
 * 
 * @author boyan
 * @Date 2011-4-22
 * @author wuhua
 * @Date 2011-9-9
 * 
 */
public class DigestLogManager {
    

    private static final Log log = LogFactory.getLog(DigestLogManager.class);
    private BrokerController brokerController;
    private final boolean startRealTimeStat = Boolean.valueOf(System
        .getProperty("meta.realtime.stat", "true"));
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "DigestLogPrintSchedule");
            }
        });
    private final PutStatsMoniter putStatsMoniter = new PutStatsMoniter(brokerController);
    private final GetStatsMoniter getStatsMoniter = new GetStatsMoniter(brokerController);
    private final StoreStatsMoniter storeStatsMoniter = new StoreStatsMoniter(brokerController);

    public DigestLogManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        if (startRealTimeStat) {
            scheduler.scheduleWithFixedDelay(new DigestPrintOut(), 10, 20, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    scheduler.shutdown();
                }
            });
            log.warn("实时统计启动...");
        }
    }


    public void dispose() {
        scheduler.shutdown();
    }

    class DigestPrintOut implements Runnable {
        @Override
        public void run() {
            putStatsMoniter.tolog();
            getStatsMoniter.tolog();
            storeStatsMoniter.tolog();
//            transationStatsMoniter.tolog();
        }
    }

    public PutStatsMoniter getPutStatsMoniter() {
        return putStatsMoniter;
    }


    public StoreStatsMoniter getStoreStatsMoniter() {
        return storeStatsMoniter;
    }

    public GetStatsMoniter getGetStatsMoniter() {
        return getStatsMoniter;
    }

}
