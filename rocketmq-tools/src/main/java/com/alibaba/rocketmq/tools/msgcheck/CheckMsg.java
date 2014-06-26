package com.alibaba.rocketmq.tools.msgcheck;

/**
 * @auther lansheng.zj
 */
public class CheckMsg {

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out
                .println("need param:CommitLogStorePath CommitLogMapedFileSize ConsumeQueueStorePathParent ConsumeQueueMapedFileSize");
            System.exit(-1);
        }

        Store store = new Store(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]));
        store.load();
        store.traval();
    }

}
