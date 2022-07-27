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

package org.apache.rocketmq.container;

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AddBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.RemoveBrokerRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.WrappedChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerContainerProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerContainer brokerContainer;
    private List<BrokerBootHook> brokerBootHookList;

    public BrokerContainerProcessor(BrokerContainer brokerContainer) {
        this.brokerContainer = brokerContainer;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        WrappedChannelHandlerContext wrappedCtx = new WrappedChannelHandlerContext(ctx);
        switch (request.getCode()) {
            case RequestCode.ADD_BROKER:
                return this.addBroker(request, wrappedCtx);
            case RequestCode.REMOVE_BROKER:
                return this.removeBroker(request, wrappedCtx);
            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(request, wrappedCtx);
            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(request, wrappedCtx);
            default:
                break;
        }
        return null;
    }

    @Override public boolean rejectRequest() {
        return false;
    }

    private synchronized RemotingCommand addBroker(RemotingCommand request, WrappedChannelHandlerContext wrappedCtx) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final AddBrokerRequestHeader requestHeader = (AddBrokerRequestHeader) request.decodeCommandCustomHeader(AddBrokerRequestHeader.class);

        LOGGER.info("addBroker called by {}", wrappedCtx.channelRemoteAddr());

        Properties brokerProperties = null;
        String configPath = requestHeader.getConfigPath();

        if (configPath != null && !configPath.isEmpty()) {
            BrokerStartup.SystemConfigFileHelper configFileHelper = new BrokerStartup.SystemConfigFileHelper();
            configFileHelper.setFile(configPath);

            try {
                brokerProperties = configFileHelper.loadConfig();
            } catch (Exception e) {
                LOGGER.error("addBroker load config from {} failed, {}", configPath, e);
            }
        } else {
            byte[] body = request.getBody();
            if (body != null) {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                brokerProperties = MixAll.string2Properties(bodyStr);
            }
        }

        if (brokerProperties == null) {
            LOGGER.error("addBroker properties empty");
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("addBroker properties empty");
            return response;
        }

        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        MixAll.properties2Object(brokerProperties, brokerConfig);
        MixAll.properties2Object(brokerProperties, messageStoreConfig);

        messageStoreConfig.setHaListenPort(brokerConfig.getListenPort() + 1);

        if (configPath != null && !configPath.isEmpty()) {
            brokerConfig.setBrokerConfigPath(configPath);
        }

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            if (!brokerConfig.isEnableControllerMode()) {
                switch (messageStoreConfig.getBrokerRole()) {
                    case ASYNC_MASTER:
                    case SYNC_MASTER:
                        brokerConfig.setBrokerId(MixAll.MASTER_ID);
                        break;
                    case SLAVE:
                        if (brokerConfig.getBrokerId() <= 0) {
                            response.setCode(ResponseCode.SYSTEM_ERROR);
                            response.setRemark("slave broker id must be > 0");
                            return response;
                        }
                        break;
                    default:
                        break;

                }
            }

            if (messageStoreConfig.getTotalReplicas() < messageStoreConfig.getInSyncReplicas()
                    || messageStoreConfig.getTotalReplicas() < messageStoreConfig.getMinInSyncReplicas()
                    || messageStoreConfig.getInSyncReplicas() < messageStoreConfig.getMinInSyncReplicas()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("invalid replicas number");
                return response;
            }
        }

        BrokerController brokerController;
        try {
            brokerController = this.brokerContainer.addBroker(brokerConfig, messageStoreConfig);
        } catch (Exception e) {
            LOGGER.error("addBroker exception {}", e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }
        if (brokerController != null) {
            brokerController.getConfiguration().registerConfig(brokerProperties);
            try {
                for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                    brokerBootHook.executeBeforeStart(brokerController, brokerProperties);
                }
                brokerController.start();

                for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                    brokerBootHook.executeAfterStart(brokerController, brokerProperties);
                }
            } catch (Exception e) {
                LOGGER.error("start broker exception {}", e);
                BrokerIdentity brokerIdentity;
                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    brokerIdentity = new BrokerIdentity(brokerConfig.getBrokerClusterName(),
                        brokerConfig.getBrokerName(), Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)));
                } else {
                    brokerIdentity = new BrokerIdentity(brokerConfig.getBrokerClusterName(),
                        brokerConfig.getBrokerName(), brokerConfig.getBrokerId());
                }
                this.brokerContainer.removeBroker(brokerIdentity);
                brokerController.shutdown();
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("start broker failed, " + e);
                return response;
            }
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("add broker return null");
        }

        return response;
    }

    private synchronized RemotingCommand removeBroker(RemotingCommand request, WrappedChannelHandlerContext wrappedCtx) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RemoveBrokerRequestHeader requestHeader = (RemoveBrokerRequestHeader) request.decodeCommandCustomHeader(RemoveBrokerRequestHeader.class);

        LOGGER.info("removeBroker called by {}", wrappedCtx.channelRemoteAddr());

        BrokerIdentity brokerIdentity = new BrokerIdentity(requestHeader.getBrokerClusterName(), requestHeader.getBrokerName(), requestHeader.getBrokerId());

        BrokerController brokerController;
        try {
            brokerController = this.brokerContainer.removeBroker(brokerIdentity);
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }

        if (brokerController != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.BROKER_NOT_EXIST);
            response.setRemark("Broker not exist");
        }
        return response;
    }

    public void registerBrokerBootHook(List<BrokerBootHook> brokerBootHookList) {
        this.brokerBootHookList = brokerBootHookList;
    }

    private RemotingCommand updateBrokerConfig(RemotingCommand request, WrappedChannelHandlerContext wrappedCtx) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        LOGGER.info("updateSharedBrokerConfig called by {}", wrappedCtx.channelRemoteAddr());

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    LOGGER.info("updateSharedBrokerConfig, new config: [{}] client: {} ", properties, wrappedCtx.remoteAddress());
                    this.brokerContainer.getConfiguration().update(properties);
                } else {
                    LOGGER.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerConfig(RemotingCommand request, WrappedChannelHandlerContext wrappedCtx) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerContainer.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerContainer.getConfiguration().getDataVersionJson());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
