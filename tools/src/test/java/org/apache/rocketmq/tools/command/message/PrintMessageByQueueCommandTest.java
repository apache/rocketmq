package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by pigpdong on 2019/2/26.
 *
 * a test for PrintMessageByQueueCommand
 * PrintMessageByQueueCommand is a command that can print message from the queue with the command printMsgByQueue on the shell console
 *
 * the method calculateByTag is a tool to  group by tag from the messages from queue
 * the method printCalculateByTag print the every tag and the count with the tag
 *
 * this command may has the below options
 * t : topic name ,required
 * a : broker name  ,required
 * q : queue id ,required
 * c : required ,optional
 * s : expression eg: TagA || TagB ,optional
 * b : beginTimestamp yyyy-MM-dd#HH:mm:ss:SSS  ,optional
 * e : endTimestamp  yyyy-MM-dd#HH:mm:ss:SSS   ,optional
 * p : print msg true or false  ,optional
 * d : printBody true or false  ,optional
 * f : calculate by tag  ,optional
 */
public class PrintMessageByQueueCommandTest {

    private static PrintMessageByQueueCommand printMessageByQueueCommand ;

    private static long midTimestamp = 0L;


    /**
     * init command with the mock class DefaultMQPullConsumer which mock the method pull message from queue
     *
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    @BeforeClass
    public static void init() throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
            NoSuchFieldException, IllegalAccessException, UnknownHostException {

        printMessageByQueueCommand = new PrintMessageByQueueCommand();

        // mock a DefaultMQPullConsumer
        DefaultMQPullConsumer consumer = mock(DefaultMQPullConsumer.class);

        //  consumer.minOffset(mq);  return 0;
        when(consumer.minOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(0));

        //  consumer.maxOffset(mq);  return 100;
        when(consumer.maxOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(100));


        //  consumer.searchOffset(mq);  return 0; when timestamp < midTimestamp
        when(consumer.searchOffset(any(MessageQueue.class), longThat(new ArgumentMatcher<Long>() {
            @Override
            public boolean matches(Long aLong) {
                if(aLong < midTimestamp)
                    return true;
                return false;
            }
        }))).thenReturn(Long.valueOf(0));

        //  consumer.searchOffset(mq);  return 50; when timestamp > midTimestamp
        when(consumer.searchOffset(any(MessageQueue.class), longThat(new ArgumentMatcher<Long>() {
            @Override
            public boolean matches(Long aLong) {
                if(aLong < midTimestamp)
                    return true;
                return false;
            }
        }))).thenReturn(Long.valueOf(50));


        SocketAddress host = new InetSocketAddress(InetAddress.getLocalHost(),3300);
        long currentTime = System.currentTimeMillis();

        // mock msgList and PullRequest
        List<MessageExt> allMsgList = new ArrayList<>();

        for(int t=0; t<100; t++){
            MessageExt msg = new MessageExt(t,currentTime,host,currentTime,host,""+t);
            msg.setBody(new String("msg"+t).getBytes());
            if(t % 2 == 0)
                msg.setTags("pigpdong");
            allMsgList.add(msg);
        }

        final PullResult pullResult = new PullResult(PullStatus.FOUND, 100, 0, 100, allMsgList);

        when(consumer.pull(any(MessageQueue.class), anyString(), anyLong(), anyInt())).thenReturn(pullResult);

        Field consumerField = PrintMessageByQueueCommand.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(printMessageByQueueCommand, consumer);
    }

    @Test
    public void testExecuteDefault() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] subargs = new String[] {"-t mytopic", "-a brokera", "-i 4", "-p true", "-d true", "-f true"};

        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + printMessageByQueueCommand.commandName(),
                subargs, printMessageByQueueCommand.buildCommandlineOptions(options), new PosixParser());

        printMessageByQueueCommand.execute(commandLine, options, null);

        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("MSGID: 0"));
    }



    //you may use this main method to use the console input
    public static void main(String[] args) throws Exception{
        Scanner sc = new Scanner(System.in);

        // -t mytopic  -a brokera -i 4 -p true -d true -f true
        String input = sc.nextLine().trim();
        String[] commandargs = input.split("-");
        String[] subargs = new String[commandargs.length - 1];
        for(int t=1;t<commandargs.length;t++){
            subargs[t-1] = "-" + commandargs[t].trim();
        }

        init();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + printMessageByQueueCommand.commandName(),
                subargs, printMessageByQueueCommand.buildCommandlineOptions(options), new PosixParser());

        printMessageByQueueCommand.execute(commandLine, options, null);

    }



    }
