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
package org.apache.rocketmq.auth.authentication.builder;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthenticationContextBuilder implements AuthenticationContextBuilder<DefaultAuthenticationContext> {

    private static final String CREDENTIAL = "Credential";
    private static final String SIGNATURE = "Signature";

    @Override
    public DefaultAuthenticationContext build(Metadata metadata, GeneratedMessageV3 request) {
        try {
            DefaultAuthenticationContext context = new DefaultAuthenticationContext();
            context.setRpcCode(request.getDescriptorForType().getFullName());
            String authorization = metadata.get(GrpcConstants.AUTHORIZATION);
            if (StringUtils.isEmpty(authorization)) {
                throw new AuthenticationException("authentication header is null");
            }
            String datetime = metadata.get(GrpcConstants.DATE_TIME);
            if (StringUtils.isEmpty(datetime)) {
                throw new AuthenticationException("datetime is null");
            }

            String[] result = authorization.split(CommonConstants.SPACE, 2);
            if (result.length != 2) {
                throw new AuthenticationException("authentication header is incorrect");
            }
            String[] keyValues = result[1].split(CommonConstants.COMMA);
            for (String keyValue : keyValues) {
                String[] kv = keyValue.trim().split(CommonConstants.EQUAL, 2);
                int kvLength = kv.length;
                if (kv.length != 2) {
                    throw new AuthenticationException("authentication keyValues length is incorrect, actual length=" + kvLength);
                }
                String authItem = kv[0];
                if (CREDENTIAL.equals(authItem)) {
                    String[] credential = kv[1].split(CommonConstants.SLASH);
                    int credentialActualLength = credential.length;
                    if (credentialActualLength == 0) {
                        throw new AuthenticationException("authentication credential length is incorrect, actual length=" + credentialActualLength);
                    }
                    context.setUsername(credential[0]);
                    continue;
                }
                if (SIGNATURE.equals(authItem)) {
                    context.setSignature(this.hexToBase64(kv[1]));
                }
            }

            context.setContent(datetime.getBytes(StandardCharsets.UTF_8));

            return context;
        } catch (AuthenticationException e) {
            throw e;
        } catch (Throwable e) {
            throw new AuthenticationException("create authentication context error.", e);
        }
    }

    @Override
    public DefaultAuthenticationContext build(RemotingCommand request) {
        HashMap<String, String> fields = request.getExtFields();
        if (MapUtils.isEmpty(fields)) {
            throw new AuthenticationException("authentication field is null");
        }
        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode(String.valueOf(request.getCode()));
        context.setUsername(fields.get(SessionCredentials.ACCESS_KEY));
        context.setSignature(fields.get(SessionCredentials.SIGNATURE));
        // Content
        SortedMap<String, String> map = new TreeMap<>();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (request.getVersion() <= MQVersion.Version.V4_9_3.ordinal() &&
                MixAll.UNIQUE_MSG_QUERY_FLAG.equals(entry.getKey())) {
                continue;
            }
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        context.setContent(AclUtils.combineRequestContent(request, map));
        return context;
    }

    public String hexToBase64(String input) throws DecoderException {
        byte[] bytes = Hex.decodeHex(input);
        return Base64.encodeBase64String(bytes);
    }
}
