/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.example.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class TransactionProducer {
    private static final long START_TIME = System.currentTimeMillis();
    private static final LongAdder MSG_COUNT = new LongAdder();

    //broker max check times should less than this value
    static final int MAX_CHECK_RESULT_IN_MSG = 20;

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {
        System.setProperty(RemotingCommand.SERIALIZE_TYPE_PROPERTY, SerializeType.ROCKETMQ.name());
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("TransactionProducer", args, buildCommandlineOptions(options), new DefaultParser());
        TxSendConfig config = new TxSendConfig();
        config.topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        config.threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 32;
        config.messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 2048;
        config.sendRollbackRate = commandLine.hasOption("sr") ? Double.parseDouble(commandLine.getOptionValue("sr")) : 0.0;
        config.sendUnknownRate = commandLine.hasOption("su") ? Double.parseDouble(commandLine.getOptionValue("su")) : 0.0;
        config.checkRollbackRate = commandLine.hasOption("cr") ? Double.parseDouble(commandLine.getOptionValue("cr")) : 0.0;
        config.checkUnknownRate = commandLine.hasOption("cu") ? Double.parseDouble(commandLine.getOptionValue("cu")) : 0.0;
        config.batchId = commandLine.hasOption("b") ? Long.parseLong(commandLine.getOptionValue("b")) : System.currentTimeMillis();
        config.sendInterval = commandLine.hasOption("i") ? Integer.parseInt(commandLine.getOptionValue("i")) : 0;
        config.aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));
        config.msgTraceEnable = commandLine.hasOption('m') && Boolean.parseBoolean(commandLine.getOptionValue('m'));
        config.reportInterval = commandLine.hasOption("ri") ? Integer.parseInt(commandLine.getOptionValue("ri")) : 10000;

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(config.threadCount);

        final StatsBenchmarkTProducer statsBenchmark = new StatsBenchmarkTProducer();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());

        final LinkedList<Snapshot> snapshotList = new LinkedList<>();

        executorService.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                while (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Snapshot begin = snapshotList.getFirst();
                    Snapshot end = snapshotList.getLast();

                    final long sendCount = end.sendRequestSuccessCount - begin.sendRequestSuccessCount;
                    final long sendTps = (sendCount * 1000L) / (end.endTime - begin.endTime);
                    final double averageRT = (end.sendMessageTimeTotal - begin.sendMessageTimeTotal) / (double) (end.sendRequestSuccessCount - begin.sendRequestSuccessCount);

                    final long failCount = end.sendRequestFailedCount - begin.sendRequestFailedCount;
                    final long checkCount = end.checkCount - begin.checkCount;
                    final long unexpectedCheck = end.unexpectedCheckCount - begin.unexpectedCheckCount;
                    final long dupCheck = end.duplicatedCheck - begin.duplicatedCheck;

                    System.out.printf(
                        "Current Time: %s | Send TPS: %5d | Max RT(ms): %5d | AVG RT(ms): %3.1f | Send Failed: %d | Check: %d | UnexpectedCheck: %d | DuplicatedCheck: %d%n",
                        UtilAll.timeMillisToHumanString2(System.currentTimeMillis()), sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, failCount, checkCount,
                        unexpectedCheck, dupCheck);
                    statsBenchmark.getSendMessageMaxRT().set(0);
                }
            }

            @Override
            public void run() {
                try {
                    this.printStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, config.reportInterval, config.reportInterval, TimeUnit.MILLISECONDS);

        RPCHook rpcHook = null;
        if (config.aclEnable) {
            String ak = commandLine.hasOption("ak") ? String.valueOf(commandLine.getOptionValue("ak")) : AclClient.ACL_ACCESS_KEY;
            String sk = commandLine.hasOption("sk") ? String.valueOf(commandLine.getOptionValue("sk")) : AclClient.ACL_SECRET_KEY;
            rpcHook = AclClient.getAclRPCHook(ak, sk);
        }
        final TransactionListener transactionCheckListener = new TransactionListenerImpl(statsBenchmark, config);
        final TransactionMQProducer producer = new TransactionMQProducer(
            "benchmark_transaction_producer",
            rpcHook,
            config.msgTraceEnable,
            null);
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setTransactionListener(transactionCheckListener);
        producer.setDefaultTopicQueueNums(1000);
        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }
        producer.start();

        for (int i = 0; i < config.threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        boolean success = false;
                        final long beginTimestamp = System.currentTimeMillis();
                        try {
                            SendResult sendResult =
                                producer.sendMessageInTransaction(buildMessage(config), null);
                            success = sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK;
                        } catch (Throwable e) {
                            success = false;
                        } finally {
                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageTimeTotal().add(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                boolean updated = statsBenchmark.getSendMessageMaxRT()
                                    .compareAndSet(prevMaxRT, currentRT);
                                if (updated)
                                    break;

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                            if (success) {
                                statsBenchmark.getSendRequestSuccessCount().increment();
                            } else {
                                statsBenchmark.getSendRequestFailedCount().increment();
                            }
                            if (config.sendInterval > 0) {
                                try {
                                    Thread.sleep(config.sendInterval);
                                } catch (InterruptedException e) {
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    private static Message buildMessage(TxSendConfig config) {
        byte[] bs = new byte[config.messageSize];
        ThreadLocalRandom r = ThreadLocalRandom.current();
        r.nextBytes(bs);

        ByteBuffer buf = ByteBuffer.wrap(bs);
        buf.putLong(config.batchId);
        long sendMachineId = START_TIME << 32;
        long count = MSG_COUNT.longValue();
        long msgId = sendMachineId | count;
        MSG_COUNT.increment();
        buf.putLong(msgId);

        // save send tx result in message
        if (r.nextDouble() < config.sendRollbackRate) {
            buf.put((byte) LocalTransactionState.ROLLBACK_MESSAGE.ordinal());
        } else if (r.nextDouble() < config.sendUnknownRate) {
            buf.put((byte) LocalTransactionState.UNKNOW.ordinal());
        } else {
            buf.put((byte) LocalTransactionState.COMMIT_MESSAGE.ordinal());
        }

        // save check tx result in message
        for (int i = 0; i < MAX_CHECK_RESULT_IN_MSG; i++) {
            if (r.nextDouble() < config.checkRollbackRate) {
                buf.put((byte) LocalTransactionState.ROLLBACK_MESSAGE.ordinal());
            } else if (r.nextDouble() < config.checkUnknownRate) {
                buf.put((byte) LocalTransactionState.UNKNOW.ordinal());
            } else {
                buf.put((byte) LocalTransactionState.COMMIT_MESSAGE.ordinal());
            }
        }

        Message msg = new Message();
        msg.setTopic(config.topic);

        msg.setBody(bs);
        return msg;
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("w", "threadCount", true, "Thread count, Default: 32");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 2048");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sr", "send rollback rate", true, "Send rollback rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("su", "send unknown rate", true, "Send unknown rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("cr", "check rollback rate", true, "Check rollback rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("cu", "check unknown rate", true, "Check unknown rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "test batch id", true, "test batch id, Default: System.currentMillis()");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "send interval", true, "sleep interval in millis between messages, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "aclEnable", true, "Acl Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ak", "accessKey", true, "Acl access key, Default: 12345678");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sk", "secretKey", true, "Acl secret key, Default: rocketmq2");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "msgTraceEnable", true, "Message Trace Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ri", "reportInterval", true, "The number of ms between reports, Default: 10000");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}

class TransactionListenerImpl implements TransactionListener {
    private StatsBenchmarkTProducer statBenchmark;
    private TxSendConfig sendConfig;
    private final LRUMap<Long, Integer> cache = new LRUMap<>(200000);

    private class MsgMeta {
        long batchId;
        long msgId;
        LocalTransactionState sendResult;
        List<LocalTransactionState> checkResult;
    }

    public TransactionListenerImpl(StatsBenchmarkTProducer statsBenchmark, TxSendConfig sendConfig) {
        this.statBenchmark = statsBenchmark;
        this.sendConfig = sendConfig;
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return parseFromMsg(msg).sendResult;
    }

    private MsgMeta parseFromMsg(Message msg) {
        byte[] bs = msg.getBody();
        ByteBuffer buf = ByteBuffer.wrap(bs);
        MsgMeta msgMeta = new MsgMeta();
        msgMeta.batchId = buf.getLong();
        msgMeta.msgId = buf.getLong();
        msgMeta.sendResult = LocalTransactionState.values()[buf.get()];
        msgMeta.checkResult = new ArrayList<>();
        for (int i = 0; i < TransactionProducer.MAX_CHECK_RESULT_IN_MSG; i++) {
            msgMeta.checkResult.add(LocalTransactionState.values()[buf.get()]);
        }
        return msgMeta;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        MsgMeta msgMeta = parseFromMsg(msg);
        if (msgMeta.batchId != sendConfig.batchId) {
            // message not generated in this test
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        statBenchmark.getCheckCount().increment();

        int times = 0;
        try {
            String checkTimes = msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
            times = Integer.parseInt(checkTimes);
        } catch (Exception e) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        times = times <= 0 ? 1 : times;

        boolean dup;
        synchronized (cache) {
            Integer oldCheckLog = cache.get(msgMeta.msgId);
            Integer newCheckLog;
            if (oldCheckLog == null) {
                newCheckLog = 1 << (times - 1);
            } else {
                newCheckLog = oldCheckLog | (1 << (times - 1));
            }
            dup = newCheckLog.equals(oldCheckLog);
        }
        if (dup) {
            statBenchmark.getDuplicatedCheckCount().increment();
        }
        if (msgMeta.sendResult != LocalTransactionState.UNKNOW) {
            System.out.printf("%s unexpected check: msgId=%s,txId=%s,checkTimes=%s,sendResult=%s\n",
                new SimpleDateFormat("HH:mm:ss,SSS").format(new Date()),
                msg.getMsgId(), msg.getTransactionId(),
                msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES),
                msgMeta.sendResult.toString());
            statBenchmark.getUnexpectedCheckCount().increment();
            return msgMeta.sendResult;
        }

        for (int i = 0; i < times - 1; i++) {
            LocalTransactionState s = msgMeta.checkResult.get(i);
            if (s != LocalTransactionState.UNKNOW) {
                System.out.printf("%s unexpected check: msgId=%s,txId=%s,checkTimes=%s,sendResult,lastCheckResult=%s\n",
                    new SimpleDateFormat("HH:mm:ss,SSS").format(new Date()),
                    msg.getMsgId(), msg.getTransactionId(),
                    msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES), s);
                statBenchmark.getUnexpectedCheckCount().increment();
                return s;
            }
        }
        return msgMeta.checkResult.get(times - 1);
    }
}

class Snapshot {
    long endTime;

    long sendRequestSuccessCount;

    long sendRequestFailedCount;

    long sendMessageTimeTotal;

    long sendMessageMaxRT;

    long checkCount;

    long unexpectedCheckCount;

    long duplicatedCheck;
}

class StatsBenchmarkTProducer {
    private final LongAdder sendRequestSuccessCount = new LongAdder();

    private final LongAdder sendRequestFailedCount = new LongAdder();

    private final LongAdder sendMessageTimeTotal = new LongAdder();

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    private final LongAdder checkCount = new LongAdder();

    private final LongAdder unexpectedCheckCount = new LongAdder();

    private final LongAdder duplicatedCheckCount = new LongAdder();

    public Snapshot createSnapshot() {
        Snapshot s = new Snapshot();
        s.endTime = System.currentTimeMillis();
        s.sendRequestSuccessCount = sendRequestSuccessCount.longValue();
        s.sendRequestFailedCount = sendRequestFailedCount.longValue();
        s.sendMessageTimeTotal = sendMessageTimeTotal.longValue();
        s.sendMessageMaxRT = sendMessageMaxRT.get();
        s.checkCount = checkCount.longValue();
        s.unexpectedCheckCount = unexpectedCheckCount.longValue();
        s.duplicatedCheck = duplicatedCheckCount.longValue();
        return s;
    }

    public LongAdder getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public LongAdder getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public LongAdder getSendMessageTimeTotal() {
        return sendMessageTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }

    public LongAdder getCheckCount() {
        return checkCount;
    }

    public LongAdder getUnexpectedCheckCount() {
        return unexpectedCheckCount;
    }

    public LongAdder getDuplicatedCheckCount() {
        return duplicatedCheckCount;
    }
}

class TxSendConfig {
    String topic;
    int threadCount;
    int messageSize;
    double sendRollbackRate;
    double sendUnknownRate;
    double checkRollbackRate;
    double checkUnknownRate;
    long batchId;
    int sendInterval;
    boolean aclEnable;
    boolean msgTraceEnable;
    int reportInterval;
}

class LRUMap<K, V> extends LinkedHashMap<K, V> {

    private int maxSize;

    public LRUMap(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > maxSize;
    }
}
