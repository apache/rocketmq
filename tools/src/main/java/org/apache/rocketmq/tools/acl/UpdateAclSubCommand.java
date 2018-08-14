package org.apache.rocketmq.tools.acl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.broker.acl.service.AclService;
import org.apache.rocketmq.broker.acl.serviceImpl.AclServiceImpl;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateAclSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateTopicAcl";
    }

    @Override
    public String commandDesc() {
        return "add or del topic acl";

    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("U", "userId", true, "user name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("P", "password", true, "password");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("T", "targetUserId", true, "target user name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("A", "add", false, "add acl");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("D", "delete", false, "delete acl");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("O", "operate type", true, "R|W");
        opt.setRequired(true);
        options.addOption(opt);
        return null;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        String topicId = "";
        String clusterName = "";
        String userId = "";
        String pass = "";
        String targetUserId = "";
        Character type ='R';
        AclService aclService = new AclServiceImpl();
        try{
            if (commandLine.hasOption('t')) {
                topicId = commandLine.getOptionValue("t").trim();
            }else {
                return;
            }
            if (commandLine.hasOption('c')) {
                clusterName = commandLine.getOptionValue("c").trim();
            }
            if(commandLine.hasOption("U")){
                userId = commandLine.getOptionValue("U").trim();
            }else{
                return ;
            }
            if(commandLine.hasOption("P")){
                pass = commandLine.getOptionValue("P").trim();
            }else{
                return;
            }
            if(commandLine.hasOption("T")){
                targetUserId = commandLine.getOptionValue("T").trim();
            }else{
                return;
            }
            if(commandLine.hasOption("O")){
                type = commandLine.getOptionValue("O").trim().charAt(0);
            }
            if(commandLine.hasOption("A")){
                if(type.equals('R')){
                    aclService.addReadAcl(userId,pass,topicId,targetUserId);
                }else if(type.equals("W")){
                    aclService.addWriteAcl(userId,pass,topicId,targetUserId);
                }else {
                    return ;
                }
            }else if(commandLine.hasOption("D")){
                if(type.equals('R')){
                    aclService.delReadAcl(userId,pass,topicId,targetUserId);
                }else if(type.equals("W")){
                    aclService.delWriteAcl(userId,pass,topicId,targetUserId);
                }else {
                    return ;
                }
            }else{
                return ;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
