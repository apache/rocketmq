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

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.srvutil.ServerUtil;

public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("TransactionProducer", args, buildCommandlineOptions(options), new PosixParser());

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 32;
        final int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 2048;
        final boolean ischeck = commandLine.hasOption('c') ? Boolean.parseBoolean(commandLine.getOptionValue('c')) : false;
        final boolean ischeckffalse = commandLine.hasOption('r') ? Boolean.parseBoolean(commandLine.getOptionValue('r')) : true;

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

        final StatsBenchmarkTProducer statsBenchmark = new StatsBenchmarkTProducer();

        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                while (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps =
                        (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                    System.out.printf(
                        "Send TPS: %d Max RT: %d Average RT: %7.3f Send Failed: %d Response Failed: %d transaction checkCount: %d %n",
                        sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, end[2], end[4], end[6]);
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
        }, 10000, 10000);

        final TransactionCheckListener transactionCheckListener =
            new TransactionCheckListenerBImpl(ischeckffalse, statsBenchmark);
        final TransactionMQProducer producer = new TransactionMQProducer("benchmark_transaction_producer");
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setTransactionCheckListener(transactionCheckListener);
        producer.setDefaultTopicQueueNums(1000);
        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }
        producer.start();

        final TransactionExecuterBImpl tranExecuter = new TransactionExecuterBImpl(ischeck);

        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            // Thread.sleep(1000);
                            final long beginTimestamp = System.currentTimeMillis();
                            SendResult sendResult =
                                producer.sendMessageInTransaction(buildMessage(messageSize, topic), tranExecuter, null);
                            if (sendResult != null) {
                                statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                                statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
                            }

                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                boolean updated =
                                    statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT,
                                        currentRT);
                                if (updated)
                                    break;

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                        }
                    }
                }
            });
        }
    }

    private static Message buildMessage(final int messageSize, String topic) {
        try {
            Message msg = new Message();
            msg.setTopic(topic);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < messageSize; i += 10) {
                sb.append("hello baby");
            }
            msg.setBody(sb.toString().getBytes(RemotingHelper.DEFAULT_CHARSET));
            return msg;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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

        opt = new Option("c", "check", true, "Check the message, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "checkResult", true, "Message check result, Default: true");
        opt.setRequired(false);
        options.addOption(opt);


        return options;
    }
}

class TransactionExecuterBImpl implements LocalTransactionExecuter {

    private boolean ischeck;

    public TransactionExecuterBImpl(boolean ischeck) {
        this.ischeck = ischeck;
    }

    @Override
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {
        if (ischeck) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}

class TransactionCheckListenerBImpl implements TransactionCheckListener {
    private boolean ischeckffalse;
    private StatsBenchmarkTProducer statsBenchmarkTProducer;

    public TransactionCheckListenerBImpl(boolean ischeckffalse,
        StatsBenchmarkTProducer statsBenchmarkTProducer) {
        this.ischeckffalse = ischeckffalse;
        this.statsBenchmarkTProducer = statsBenchmarkTProducer;
    }

    @Override
    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        statsBenchmarkTProducer.getCheckRequestSuccessCount().incrementAndGet();
        if (ischeckffalse) {

            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }
}

class StatsBenchmarkTProducer {
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

    private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    private final AtomicLong checkRequestSuccessCount = new AtomicLong(0L);

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.sendRequestSuccessCount.get(),
            this.sendRequestFailedCount.get(),
            this.receiveResponseSuccessCount.get(),
            this.receiveResponseFailedCount.get(),
            this.sendMessageSuccessTimeTotal.get(),
            this.checkRequestSuccessCount.get()};

        return snap;
    }

    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public AtomicLong getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }

    public AtomicLong getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }

    public AtomicLong getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }

    public AtomicLong getCheckRequestSuccessCount() {
        return checkRequestSuccessCount;
    }
}
