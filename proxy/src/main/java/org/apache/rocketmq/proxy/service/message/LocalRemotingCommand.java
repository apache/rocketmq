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
package org.apache.rocketmq.proxy.service.message;

import java.util.HashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class LocalRemotingCommand extends RemotingCommand {

    public static LocalRemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader, String language) {
        LocalRemotingCommand cmd = new LocalRemotingCommand();
        cmd.setCode(code);
        cmd.setLanguage(LanguageCode.getCode(language));
        cmd.writeCustomHeader(customHeader);
        cmd.setExtFields(new HashMap<>());
        setCmdVersion(cmd);
        cmd.makeCustomHeaderToNet();
        return cmd;
    }
}
