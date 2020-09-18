package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class QueryMsgByKeySubCommandTest {

    @Ignore
    @Test
    public void testQueryWithTimeRange() throws SubCommandException, InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        long time = prepareMessageAndReturnMidTime();

        System.setProperty("rocketmq.namesrv.addr", "localhost:9876");
        QueryMsgByKeySubCommand cmd = new QueryMsgByKeySubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] subArgs = new String[]{"-n", "localhost:9876", "-t", "TopicTest", "-k", "k1", "-s", Long.toString(time)};
        final CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqadmin " + cmd.commandName(), subArgs, cmd.buildCommandlineOptions(options), new PosixParser());

        cmd.execute(commandLine, options, null);
    }

    private long prepareMessageAndReturnMidTime() throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("Producer" + System.currentTimeMillis());
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        Message msg = new Message("TopicTest", "TagA", "k1", ("Hello RocketMQ").getBytes(RemotingHelper.DEFAULT_CHARSET));

        producer.send(msg);
        long firstMsgSendTime = System.currentTimeMillis();

        Thread.sleep(100);
        producer.send(msg);
        return firstMsgSendTime;
    }

}