package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Test;

public class QueryMsgByUniqueKeySubCommandTest {

    @Test
    public void testExecute() throws SubCommandException {

        QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t topicName", "-i C0A8C79F0F786D06D69C8B0E52200000"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

        subargs = new String[] {"-t topicName", "-i C0A8C79F0F786D06D69C8B0E52200000", "-g consumerGroup", "-d clientId"};
        commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);



    }
}
