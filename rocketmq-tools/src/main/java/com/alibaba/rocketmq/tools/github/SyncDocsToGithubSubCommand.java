package com.alibaba.rocketmq.tools.github;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.kohsuke.github.GHIssue;
import org.kohsuke.github.GHOrganization;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 同步版本库中的wiki和issue到github
 */
public class SyncDocsToGithubSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "syncDocs";
    }


    @Override
    public String commandDesc() {
        return "Synchronize wiki and issue to github.com";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("u", "userName", true, "User name of github.com");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "password", true, "Password of github.com");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    private static boolean syncIssue(final GHRepository rep, final int issueId, final String body) {
        try {
            GHIssue issue = rep.getIssue(issueId);
            issue.setBody(body);
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }


    private static boolean syncWiki(final GHRepository rep, final String wikiName, final String body) {

        return false;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        String userName = commandLine.getOptionValue('u').trim();
        String password = commandLine.getOptionValue('p').trim();

        try {
            GitHub github = GitHub.connectUsingPassword(userName, password);
            GHOrganization alibaba = github.getOrganization("Alibaba");
            GHRepository rep = alibaba.getRepository("RocketMQ");

            //
            // 同步Issue
            //
            {
                File dir = new File(System.getenv(MixAll.ROCKETMQ_HOME_ENV) + "/" + "issues");
                File[] files = dir.listFiles();
                if (files != null) {
                    // ascending order
                    Arrays.sort(files);
                    for (File file : files) {
                        int issueId = Integer.parseInt(file.getName());
                        String body = MixAll.file2String(file);
                        boolean result = syncIssue(rep, issueId, body);
                        System.out.printf("Sync issue <%d> to github.com %s\n", issueId, result ? "OK"
                                : "Failed");
                    }
                }
            }

            // 同步Wiki
            {
                File dir = new File(System.getenv(MixAll.ROCKETMQ_HOME_ENV) + "/" + "wiki");
                File[] files = dir.listFiles();
                if (files != null) {
                    // ascending order
                    Arrays.sort(files);
                    for (File file : files) {
                        String fileName = file.getName();
                        int index = fileName.lastIndexOf('.');
                        if (index > 0) {
                            fileName = fileName.substring(0, index);
                        }

                        String body = MixAll.file2String(file);
                        boolean result = syncWiki(rep, fileName, body);
                        System.out.printf("Sync wiki <%s> to github.com %s\n", fileName, result ? "OK"
                                : "Failed");
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}