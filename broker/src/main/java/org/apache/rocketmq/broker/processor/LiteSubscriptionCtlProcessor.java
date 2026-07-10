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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.lite.LiteQuotaException;
import org.apache.rocketmq.broker.lite.LiteMetadataUtil;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LitePatternMatcher;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiteSubscriptionCtlProcessor implements NettyRequestProcessor {
    protected final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    private final BrokerController brokerController;
    private final LiteSubscriptionRegistry liteSubscriptionRegistry;

    public LiteSubscriptionCtlProcessor(BrokerController brokerController, LiteSubscriptionRegistry liteSubscriptionRegistry) {
        this.brokerController = brokerController;
        this.liteSubscriptionRegistry = liteSubscriptionRegistry;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (request.getBody() == null) {
            return RemotingCommand.createResponseCommand(ResponseCode.ILLEGAL_OPERATION,
                "Request body is null.");
        }

        final LiteSubscriptionCtlRequestBody requestBody = LiteSubscriptionCtlRequestBody
            .decode(request.getBody(), LiteSubscriptionCtlRequestBody.class);

        Set<LiteSubscriptionDTO> entrySet = requestBody.getSubscriptionSet();
        if (CollectionUtils.isEmpty(entrySet)) {
            return RemotingCommand.createResponseCommand(ResponseCode.ILLEGAL_OPERATION,
                "LiteSubscriptionCtlRequestBody is empty.");
        }

        try {
            for (LiteSubscriptionDTO entry : entrySet) {
                final String clientId = entry.getClientId();
                final String group = entry.getGroup();
                final String topic = entry.getTopic();
                if (StringUtils.isBlank(clientId)) {
                    log.warn("clientId is blank, {}", entry);
                    continue;
                }
                if (StringUtils.isBlank(group)) {
                    log.warn("group is blank, {}", entry);
                    continue;
                }
                if (StringUtils.isBlank(topic)) {
                    log.warn("topic is blank, {}", entry);
                    continue;
                }
                final Set<String> lmqNameSet = toLmqNameSet(entry);
                switch (entry.getAction()) {
                    case PARTIAL_ADD:
                        checkConsumeEnable(group);
                        this.liteSubscriptionRegistry.updateClientChannel(clientId, ctx.channel());
                        this.liteSubscriptionRegistry.addPartialSubscription(clientId, group, topic, lmqNameSet, entry.getOffsetOption());
                        break;
                    case PARTIAL_REMOVE:
                        this.liteSubscriptionRegistry.removePartialSubscription(clientId, group, topic, lmqNameSet);
                        break;
                    case COMPLETE_ADD:
                        checkConsumeEnable(group);
                        this.liteSubscriptionRegistry.updateClientChannel(clientId, ctx.channel());
                        if (LiteMetadataUtil.isWildcardGroup(group, brokerController)) {
                            // For a wildcard group, liteTopicSet carries pattern strings (not literal
                            // lmqNames); pass them through verbatim and let the registry expand them.
                            // lmqNameSet is intentionally empty: pattern-mode registration ignores it
                            // (lmqNames are derived by expanding the patterns), and passing a set of
                            // pattern-derived pseudo-lmqNames here would be misleading.
                            Set<String> wildcardPatterns = toWildcardPatterns(entry);
                            this.liteSubscriptionRegistry.addCompleteSubscription(clientId, group, topic,
                                Collections.emptySet(), wildcardPatterns, entry.getVersion());
                        } else {
                            this.liteSubscriptionRegistry.addCompleteSubscription(clientId, group, topic, lmqNameSet,
                                entry.getVersion());
                        }
                        break;
                    case COMPLETE_REMOVE:
                        this.liteSubscriptionRegistry.removeCompleteSubscription(clientId);
                        break;
                }
            }
            return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        } catch (LiteQuotaException e) {
            return RemotingCommand.createResponseCommand(ResponseCode.LITE_SUBSCRIPTION_QUOTA_EXCEEDED, e.toString());
        } catch (IllegalStateException e) {
            return RemotingCommand.createResponseCommand(ResponseCode.ILLEGAL_OPERATION, e.toString());
        } catch (Exception e) {
            log.error("LiteSubscriptionCtlProcessor error", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, e.toString());
        }
    }

    private void checkConsumeEnable(String group) {
        if (!LiteMetadataUtil.isConsumeEnable(group, brokerController)) {
            throw new IllegalStateException("Consumer group is not allowed to consume.");
        }
    }

    private Set<String> toLmqNameSet(LiteSubscriptionDTO liteSubscriptionDTO) {
        if (CollectionUtils.isEmpty(liteSubscriptionDTO.getLiteTopicSet())) {
            return Collections.emptySet();
        }
        return liteSubscriptionDTO.getLiteTopicSet().stream()
            .map(liteTopic -> LiteUtil.toLmqName(liteSubscriptionDTO.getTopic(), liteTopic))
            .collect(Collectors.toSet());
    }

    /**
     * Extract wildcard patterns from a wildcard group's {@code liteTopicSet}. Pattern strings are
     * passed through verbatim (they contain {@code *} / {@code **} and must not be converted to
     * lmqNames); entries that fail {@link LitePatternMatcher#validate(String)} are dropped with a
     * warning rather than aborting the whole request.
     *
     * <p>A genuinely empty {@code liteTopicSet} means the group runs in legacy wildcard mode
     * (receive all lite-topics under the parent topic). But a <em>non-empty</em> set in which every
     * pattern is invalid must NOT silently widen into legacy/full-wildcard consumption — that would
     * turn a malformed restrictive subscription into receiving everything. Such input is rejected
     * with an {@link IllegalStateException} (mapped to {@code ILLEGAL_OPERATION}).
     *
     * @return the validated patterns; empty only when the input {@code liteTopicSet} was empty
     * @throws IllegalStateException if the input was non-empty but contained no valid patterns
     */
    private Set<String> toWildcardPatterns(LiteSubscriptionDTO liteSubscriptionDTO) {
        Set<String> raw = liteSubscriptionDTO.getLiteTopicSet();
        if (CollectionUtils.isEmpty(raw)) {
            return Collections.emptySet();
        }
        Set<String> patterns = raw.stream()
            .filter(pattern -> {
                if (LitePatternMatcher.validate(pattern)) {
                    return true;
                }
                log.warn("drop invalid wildcard pattern, group:{}, topic:{}, pattern:{}",
                    liteSubscriptionDTO.getGroup(), liteSubscriptionDTO.getTopic(), pattern);
                return false;
            })
            .collect(Collectors.toSet());
        if (patterns.isEmpty()) {
            // Non-empty input but no valid pattern: reject rather than widen to receive-all.
            throw new IllegalStateException("all wildcard patterns are invalid, group:"
                + liteSubscriptionDTO.getGroup() + ", topic:" + liteSubscriptionDTO.getTopic());
        }
        return patterns;
    }

}
