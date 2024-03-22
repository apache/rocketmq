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

package org.apache.rocketmq.proxy.service.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.broker.auth.converter.AclConverter;
import org.apache.rocketmq.broker.auth.converter.UserConverter;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractCacheLoader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class ClusterMetadataService extends AbstractStartAndShutdown implements MetadataService {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final long DEFAULT_TIMEOUT = 3000;

    private final TopicRouteService topicRouteService;
    private final MQClientAPIFactory mqClientAPIFactory;

    protected final ThreadPoolExecutor cacheRefreshExecutor;

    protected final LoadingCache<String, TopicConfigAndQueueMapping> topicConfigCache;
    protected final static TopicConfigAndQueueMapping EMPTY_TOPIC_CONFIG = new TopicConfigAndQueueMapping();

    protected final LoadingCache<String, SubscriptionGroupConfig> subscriptionGroupConfigCache;
    protected final static SubscriptionGroupConfig EMPTY_SUBSCRIPTION_GROUP_CONFIG = new SubscriptionGroupConfig();

    protected final LoadingCache<String, User> userCache;

    protected final static User EMPTY_USER = new User();

    protected final LoadingCache<String, Acl> aclCache;

    protected final static Acl EMPTY_ACL = new Acl();


    public ClusterMetadataService(TopicRouteService topicRouteService, MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        this.mqClientAPIFactory = mqClientAPIFactory;

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getMetadataThreadPoolNums(),
            config.getMetadataThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "MetadataCacheRefresh",
            config.getMetadataThreadPoolQueueCapacity()
        );
        this.topicConfigCache = CacheBuilder.newBuilder()
            .maximumSize(config.getTopicConfigCacheMaxNum())
            .expireAfterAccess(config.getTopicConfigCacheExpiredSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getTopicConfigCacheRefreshSeconds(), TimeUnit.SECONDS)
            .build(new ClusterTopicConfigCacheLoader());
        this.subscriptionGroupConfigCache = CacheBuilder.newBuilder()
            .maximumSize(config.getSubscriptionGroupConfigCacheMaxNum())
            .expireAfterAccess(config.getSubscriptionGroupConfigCacheExpiredSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getSubscriptionGroupConfigCacheRefreshSeconds(), TimeUnit.SECONDS)
            .build(new ClusterSubscriptionGroupConfigCacheLoader());
        this.userCache = CacheBuilder.newBuilder()
            .maximumSize(config.getUserCacheMaxNum())
            .expireAfterAccess(config.getUserCacheExpiredSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getUserCacheRefreshSeconds(), TimeUnit.SECONDS)
            .build(new ClusterUserCacheLoader());
        this.aclCache = CacheBuilder.newBuilder()
            .maximumSize(config.getAclCacheMaxNum())
            .expireAfterAccess(config.getAclCacheExpiredSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getAclCacheRefreshSeconds(), TimeUnit.SECONDS)
            .build(new ClusterAclCacheLoader());

        this.init();
    }

    protected void init() {
        this.appendShutdown(this.cacheRefreshExecutor::shutdown);
    }

    @Override
    public TopicMessageType getTopicMessageType(ProxyContext ctx, String topic) {
        TopicConfigAndQueueMapping topicConfigAndQueueMapping;
        try {
            topicConfigAndQueueMapping = topicConfigCache.get(topic);
        } catch (Exception e) {
            return TopicMessageType.UNSPECIFIED;
        }
        if (topicConfigAndQueueMapping.equals(EMPTY_TOPIC_CONFIG)) {
            return TopicMessageType.UNSPECIFIED;
        }
        return topicConfigAndQueueMapping.getTopicMessageType();
    }

    @Override
    public SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String group) {
        SubscriptionGroupConfig config;
        try {
            config = this.subscriptionGroupConfigCache.get(group);
        } catch (Exception e) {
            return null;
        }
        if (config == EMPTY_SUBSCRIPTION_GROUP_CONFIG) {
            return null;
        }
        return config;
    }

    @Override
    public CompletableFuture<User> getUser(ProxyContext ctx, String username) {
        CompletableFuture<User> result = new CompletableFuture<>();
        try {
            User user = this.userCache.get(username);
            if (user == EMPTY_USER) {
                user = null;
            }
            result.complete(user);
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<Acl> getAcl(ProxyContext ctx, Subject subject) {
        CompletableFuture<Acl> result = new CompletableFuture<>();
        try {
            Acl acl = this.aclCache.get(subject.getSubjectKey());
            if (acl == EMPTY_ACL) {
                acl = null;
            }
            result.complete(acl);
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    protected class ClusterSubscriptionGroupConfigCacheLoader extends AbstractCacheLoader<String, SubscriptionGroupConfig> {

        public ClusterSubscriptionGroupConfigCacheLoader() {
            super(cacheRefreshExecutor);
        }

        @Override
        protected SubscriptionGroupConfig getDirectly(String consumerGroup) throws Exception {
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            String clusterName = config.getRocketMQClusterName();
            Optional<BrokerData> brokerDataOptional = findOneBroker(clusterName);
            if (brokerDataOptional.isPresent()) {
                String brokerAddress = brokerDataOptional.get().selectBrokerAddr();
                return mqClientAPIFactory.getClient().getSubscriptionGroupConfig(brokerAddress, consumerGroup, DEFAULT_TIMEOUT);
            }
            return EMPTY_SUBSCRIPTION_GROUP_CONFIG;
        }

        @Override
        protected void onErr(String consumerGroup, Exception e) {
            log.error("load subscription config failed. consumerGroup:{}", consumerGroup, e);
        }
    }

    protected class ClusterTopicConfigCacheLoader extends AbstractCacheLoader<String, TopicConfigAndQueueMapping> {

        public ClusterTopicConfigCacheLoader() {
            super(cacheRefreshExecutor);
        }

        @Override
        protected TopicConfigAndQueueMapping getDirectly(String topic) throws Exception {
            Optional<BrokerData> brokerDataOptional = findOneBroker(topic);
            if (brokerDataOptional.isPresent()) {
                String brokerAddress = brokerDataOptional.get().selectBrokerAddr();
                return mqClientAPIFactory.getClient().getTopicConfig(brokerAddress, topic, DEFAULT_TIMEOUT);
            }
            return EMPTY_TOPIC_CONFIG;
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load topic config failed. topic:{}", key, e);
        }
    }

    protected class ClusterUserCacheLoader extends AbstractCacheLoader<String, User> {

        public ClusterUserCacheLoader() {
            super(cacheRefreshExecutor);
        }

        @Override
        protected User getDirectly(String username) throws Exception {
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            String clusterName = config.getRocketMQClusterName();
            Optional<BrokerData> brokerDataOptional = findOneBroker(clusterName);
            if (brokerDataOptional.isPresent()) {
                String brokerAddress = brokerDataOptional.get().selectBrokerAddr();
                UserInfo userInfo = mqClientAPIFactory.getClient().getUser(brokerAddress, username, DEFAULT_TIMEOUT);
                if (userInfo == null) {
                    return EMPTY_USER;
                }
                return UserConverter.convertUser(userInfo);
            }
            return EMPTY_USER;
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load user failed. username:{}", key, e);
        }
    }

    protected class ClusterAclCacheLoader extends AbstractCacheLoader<String, Acl> {

        public ClusterAclCacheLoader() {
            super(cacheRefreshExecutor);
        }

        @Override
        protected Acl getDirectly(String subject) throws Exception {
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            String clusterName = config.getRocketMQClusterName();
            Optional<BrokerData> brokerDataOptional = findOneBroker(clusterName);
            if (brokerDataOptional.isPresent()) {
                String brokerAddress = brokerDataOptional.get().selectBrokerAddr();
                AclInfo aclInfo = mqClientAPIFactory.getClient().getAcl(brokerAddress, subject, DEFAULT_TIMEOUT);
                if (aclInfo == null) {
                    return EMPTY_ACL;
                }
                return AclConverter.convertAcl(aclInfo);
            }
            return EMPTY_ACL;
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load user failed. username:{}", key, e);
        }
    }

    protected Optional<BrokerData> findOneBroker(String topic) throws Exception {
        try {
            return topicRouteService.getAllMessageQueueView(ProxyContext.createForInner(this.getClass()), topic).getTopicRouteData().getBrokerDatas().stream().findAny();
        } catch (Exception e) {
            if (TopicRouteHelper.isTopicNotExistError(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }
}
