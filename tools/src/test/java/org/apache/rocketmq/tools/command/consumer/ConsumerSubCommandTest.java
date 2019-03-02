package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.*;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.exception.*;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/***
 *  added by owen.zhang
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumerSubCommandTest {
    private static DefaultMQAdminExt defaultMQAdminExt;

    @BeforeClass
    public static void init() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        defaultMQAdminExt = mock(DefaultMQAdminExt.class);

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerConnection.setMessageModel(MessageModel.CLUSTERING);
        HashSet<Connection> connections = new HashSet<>();
        connections.add(new Connection());
        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<String, SubscriptionData>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //when(defaultMQAdminExt.examineConsumerConnectionInfo(anyString())).thenReturn(consumerConnection);

        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        //when(defaultMQAdminExt.getConsumerRunningInfo(anyString(), anyString(), anyBoolean())).thenReturn(consumerRunningInfo);
    }

    @AfterClass
    public static void terminate() {
        defaultMQAdminExt.shutdown();
    }

    @Ignore
    @Test
    public void testExecute() throws SubCommandException {
        ConsumerSubCommand consumerSubCommand = new ConsumerSubCommand();
        //consumerSubCommand.setDefaultMQAdminExt(defaultMQAdminExt);
        //TODO inject into ConsumerSubCommand

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-g unittest", "-n localhost:9876"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + consumerSubCommand.commandName(),
                subargs, consumerSubCommand.buildCommandlineOptions(options), new PosixParser());
        consumerSubCommand.execute(commandLine, options, null);
    }
}
