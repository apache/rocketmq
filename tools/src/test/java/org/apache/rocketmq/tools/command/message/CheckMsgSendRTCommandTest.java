package org.apache.rocketmq.tools.command.message;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by zhaiyi on 2018/11/8.
 */
public class CheckMsgSendRTCommandTest {
    private static CheckMsgSendRTCommand checkMsgSendRTCommand;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        checkMsgSendRTCommand = new CheckMsgSendRTCommand();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        DefaultMQAdminExt adminExt = mock(DefaultMQAdminExt.class);
        Field proField = CheckMsgSendRTCommand.class.getDeclaredField("producer");
        proField.setAccessible(true);
        proField.set(checkMsgSendRTCommand, producer);
        Field admField = CheckMsgSendRTCommand.class.getDeclaredField("adminExt");
        admField.setAccessible(true);
        admField.set(checkMsgSendRTCommand, adminExt);
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);
        when(producer.send(any(Message.class))).thenReturn(sendResult);
        when(producer.send(any(Message.class), any(MessageQueue.class))).thenReturn(sendResult);
        when(adminExt.examineTopicRouteInfo(any(String.class))).thenReturn(topicRouteData);
    }

    @AfterClass
    public static void terminal() {

    }

    @Test
    public void test() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-t TopicTest", "-a 1", "-s 100"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + checkMsgSendRTCommand.commandName(),
            subargs, checkMsgSendRTCommand.buildCommandlineOptions(options), new PosixParser());
        checkMsgSendRTCommand.execute(commandLine, options, null);
        String res = new String(outputStream.toByteArray());
        System.setOut(out);
        int index = res.lastIndexOf(":");
        res = res.substring(index + 1);
        double d = Double.parseDouble(res);
        Assert.assertNotEquals(d, Double.NaN);
        Assert.assertTrue(d > 0);
    }
}