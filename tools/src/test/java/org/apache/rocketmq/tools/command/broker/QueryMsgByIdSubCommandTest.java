package org.apache.rocketmq.tools.command.broker;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.message.QueryMsgByIdSubCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryMsgByIdSubCommandTest {

    private static  String messageId = null;

    @BeforeClass
    public static void init() throws UnknownHostException {

        //获取messageId
        SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName("localhost"),10912);
        messageId =  MessageDecoder.createMessageId(socketAddress,0);

    }


    @AfterClass
    public static void terminate() {
    }

    @Test
    public void testExecute()
            throws SubCommandException{

        //模拟查看消息
        QueryMsgByIdSubCommand queryMsgByIdSubCommand = new QueryMsgByIdSubCommand();
        Options  options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-i " +messageId};
        CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + queryMsgByIdSubCommand.commandName(), subargs, queryMsgByIdSubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByIdSubCommand.execute(commandLine, options, null);

        //模拟消费消息
        queryMsgByIdSubCommand = new QueryMsgByIdSubCommand();
        options = ServerUtil.buildCommandlineOptions(new Options());
        subargs = new String[] {"-i " +messageId,"-g consumerMessageGroup","-d consumerMessageGroup"};
        commandLine =
                ServerUtil.parseCmdLine("mqadmin " + queryMsgByIdSubCommand.commandName(), subargs, queryMsgByIdSubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByIdSubCommand.execute(commandLine, options, null);

        //模拟重新发送消息
        queryMsgByIdSubCommand = new QueryMsgByIdSubCommand();
        options = ServerUtil.buildCommandlineOptions(new Options());
        subargs = new String[] {"-i " +messageId,"-s messageTest"};
        commandLine =
                ServerUtil.parseCmdLine("mqadmin " + queryMsgByIdSubCommand.commandName(), subargs, queryMsgByIdSubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByIdSubCommand.execute(commandLine, options, null);
    }
}
