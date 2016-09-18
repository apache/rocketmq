/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.tools.github;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.kohsuke.github.GHIssue;
import org.kohsuke.github.GHOrganization;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;

import java.io.File;
import java.util.Arrays;

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

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        String userName = commandLine.getOptionValue('u').trim();
        String password = commandLine.getOptionValue('p').trim();

        try {
            GitHub github = GitHub.connectUsingPassword(userName, password);
            GHOrganization alibaba = github.getOrganization("Alibaba");
            GHRepository rep = alibaba.getRepository("RocketMQ");

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
                        System.out.printf("Sync issue <%d> to github.com %s%n", issueId, result ? "OK"
                                : "Failed");
                    }
                }
            }

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
                        System.out.printf("Sync wiki <%s> to github.com %s%n", fileName, result ? "OK"
                                : "Failed");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean syncIssue(final GHRepository rep, final int issueId, final String body) {
        try {
            GHIssue issue = rep.getIssue(issueId);
            issue.setBody(body);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    private static boolean syncWiki(final GHRepository rep, final String wikiName, final String body) {

        return false;
    }
}