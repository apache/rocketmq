package org.apache.rocketmq.namesrv.yunai;

/**
 * Created by yunai on 2017/4/15.
 */
public class Main {

    private static final String STR = "class GroupCommitService extends FlushCommitLogService {\n" +
            "        /**\n" +
            "         * 写入请求队列\n" +
            "         */\n" +
            "        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();\n" +
            "        /**\n" +
            "         * 读取请求队列\n" +
            "         */\n" +
            "        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();\n" +
            "\n" +
            "        /**\n" +
            "         * 添加写入请求\n" +
            "         *\n" +
            "         * @param request 写入请求\n" +
            "         */\n" +
            "        public synchronized void putRequest(final GroupCommitRequest request) {\n" +
            "            // 添加写入请求\n" +
            "            synchronized (this.requestsWrite) {\n" +
            "                this.requestsWrite.add(request);\n" +
            "            }\n" +
            "            // 切换读写队列\n" +
            "            if (hasNotified.compareAndSet(false, true)) {\n" +
            "                waitPoint.countDown(); // notify\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        /**\n" +
            "         * 切换读写队列\n" +
            "         */\n" +
            "        private void swapRequests() {\n" +
            "            List<GroupCommitRequest> tmp = this.requestsWrite;\n" +
            "            this.requestsWrite = this.requestsRead;\n" +
            "            this.requestsRead = tmp;\n" +
            "        }\n" +
            "\n" +
            "        private void doCommit() {\n" +
            "            synchronized (this.requestsRead) {\n" +
            "                if (!this.requestsRead.isEmpty()) {\n" +
            "                    for (GroupCommitRequest req : this.requestsRead) {\n" +
            "                        // There may be a message in the next file, so a maximum of\n" +
            "                        // two times the flush (可能批量提交的messages，分布在两个MappedFile)\n" +
            "                        boolean flushOK = false;\n" +
            "                        for (int i = 0; i < 2 && !flushOK; i++) {\n" +
            "                            // 是否满足需要flush条件，即请求的offset超过flush的offset\n" +
            "                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();\n" +
            "                            if (!flushOK) {\n" +
            "                                CommitLog.this.mappedFileQueue.flush(0);\n" +
            "                            }\n" +
            "                        }\n" +
            "                        // 唤醒等待请求\n" +
            "                        req.wakeupCustomer(flushOK);\n" +
            "                    }\n" +
            "\n" +
            "                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();\n" +
            "                    if (storeTimestamp > 0) {\n" +
            "                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);\n" +
            "                    }\n" +
            "\n" +
            "                    // 清理读取队列\n" +
            "                    this.requestsRead.clear();\n" +
            "                } else {\n" +
            "                    // Because of individual messages is set to not sync flush, it\n" +
            "                    // will come to this process 不合法的请求，比如message上未设置isWaitStoreMsgOK。\n" +
            "                    // 走到此处的逻辑，相当于发送一条消息，落盘一条消息，实际无批量提交的效果。\n" +
            "                    CommitLog.this.mappedFileQueue.flush(0);\n" +
            "                }\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        public void run() {\n" +
            "            CommitLog.log.info(this.getServiceName() + \" service started\");\n" +
            "\n" +
            "            while (!this.isStopped()) {\n" +
            "                try {\n" +
            "                    this.waitForRunning(10);\n" +
            "                    this.doCommit();\n" +
            "                } catch (Exception e) {\n" +
            "                    CommitLog.log.warn(this.getServiceName() + \" service has exception. \", e);\n" +
            "                }\n" +
            "            }\n" +
            "\n" +
            "            // Under normal circumstances shutdown, wait for the arrival of the\n" +
            "            // request, and then flush\n" +
            "            try {\n" +
            "                Thread.sleep(10);\n" +
            "            } catch (InterruptedException e) {\n" +
            "                CommitLog.log.warn(\"GroupCommitService Exception, \", e);\n" +
            "            }\n" +
            "\n" +
            "            synchronized (this) {\n" +
            "                this.swapRequests();\n" +
            "            }\n" +
            "\n" +
            "            this.doCommit();\n" +
            "\n" +
            "            CommitLog.log.info(this.getServiceName() + \" service end\");\n" +
            "        }\n" +
            "\n" +
            "        /**\n" +
            "         * 每次执行完，切换读写队列\n" +
            "         */\n" +
            "        @Override\n" +
            "        protected void onWaitEnd() {\n" +
            "            this.swapRequests();\n" +
            "        }\n" +
            "\n" +
            "        @Override\n" +
            "        public String getServiceName() {\n" +
            "            return GroupCommitService.class.getSimpleName();\n" +
            "        }\n" +
            "\n" +
            "        @Override\n" +
            "        public long getJointime() {\n" +
            "            return 1000 * 60 * 5;\n" +
            "        }\n" +
            "    }"
            ;

    public static void main(String[] args) {
        int i = 1;
        boolean replaceBlank = STR.split("\n")[0].contains("class")
                || STR.split("\n")[0].contains("interface");
        for (String str : STR.split("\n")) {
            if (replaceBlank) {
                str = str.replaceFirst("    ", "");
            }
            if (i < 10) {
                System.out.print("  " + i + ": ");
            } else if (i < 100) {
                System.out.print(" " + i + ": ");
            } else {
                System.out.print("" + i + ": ");
            }
            System.out.println(str);
            i++;
        }
    }

}
