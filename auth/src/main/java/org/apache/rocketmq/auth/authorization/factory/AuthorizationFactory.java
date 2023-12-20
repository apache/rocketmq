package org.apache.rocketmq.auth.authorization.factory;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManagerImpl;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationFactory {

    private static final ConcurrentMap<String, Object> INSTANCE_MAP = new ConcurrentHashMap<>();
    private static final String PROVIDER_PREFIX = "PROVIDER_";
    private static final String METADATA_PROVIDER_PREFIX = "METADATA_PROVIDER_";
    private static final String EVALUATOR_PREFIX = "EVALUATOR_";

    public static AuthorizationProvider getProvider(AuthConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthorizationProvider();
            if (config.isAuthorizationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authorization provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }
            AuthorizationProvider result;
            try {
                result = (AuthorizationProvider) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization provider", e);
            }
            return result;
        });
    }

    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config) {
        return getMetadataProvider(config, null);
    }

    public static AuthorizationMetadataManager getMetadataManager(AuthConfig config) {
        return new AuthorizationMetadataManagerImpl(config);
    }

    public static AuthorizationMetadataProvider getMetadataProvider(AuthConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthorizationMetadataProvider();
            if (config.isAuthorizationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authorization metadata provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }

            AuthorizationMetadataProvider result;
            try {
                result = (AuthorizationMetadataProvider) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authorization metadata provider", e);
            }
            return result;
        });
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config));
    }

    public static AuthorizationEvaluator getEvaluator(AuthConfig config, Supplier<?> metadataService) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthorizationEvaluator(config, metadataService));
    }

    public static List<AuthorizationContext> newContexts(AuthConfig config, Metadata metadata,
        GeneratedMessageV3 message) {
        AuthorizationProvider authorizationProvider = getProvider(config);
        if (authorizationProvider == null) {
            return null;
        }
        return authorizationProvider.newContexts(metadata, message);
    }

    public static List<AuthorizationContext> newContexts(AuthConfig config, RemotingCommand command,
        String remoteAddr) {
        AuthorizationProvider authorizationProvider = getProvider(config);
        if (authorizationProvider == null) {
            return null;
        }
        return authorizationProvider.newContexts(command, remoteAddr);
    }

    @SuppressWarnings("unchecked")
    private static <V> V computeIfAbsent(String key, Function<String, ? extends V> function) {
        Object value = INSTANCE_MAP.computeIfAbsent(key, function);
        return value != null ? (V) value : null;
    }
}
