package org.apache.rocketmq.auth.authentication.factory;

import io.grpc.Metadata;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManagerImpl;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthenticationFactory {

    private static final ConcurrentMap<String, Object> INSTANCE_MAP = new ConcurrentHashMap<>();
    private static final String PROVIDER_PREFIX = "PROVIDER_";
    private static final String METADATA_PROVIDER_PREFIX = "METADATA_PROVIDER_";
    private static final String EVALUATOR_PREFIX = "EVALUATOR_";

    @SuppressWarnings("unchecked")
    public static AuthenticationProvider<AuthenticationContext> getProvider(AuthConfig config) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthenticationProvider();
            if (config.isAuthenticationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authentication provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }
            AuthenticationProvider<AuthenticationContext> result;
            try {
                result = (AuthenticationProvider<AuthenticationContext>) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication provider", e);
            }
            return result;
        });
    }

    public static AuthenticationMetadataProvider getMetadataProvider(AuthConfig config) {
        return getMetadataProvider(config, null);
    }

    public static AuthenticationMetadataManager getMetadataManager(AuthConfig config) {
        return new AuthenticationMetadataManagerImpl(config);
    }

    public static AuthenticationMetadataProvider getMetadataProvider(AuthConfig config, Supplier<?> metadataService) {
        if (config == null) {
            return null;
        }
        return computeIfAbsent(METADATA_PROVIDER_PREFIX + config.getConfigName(), key -> {
            String clazzName = config.getAuthenticationMetadataProvider();
            if (config.isAuthenticationEnabled() && StringUtils.isEmpty(clazzName)) {
                throw new RuntimeException("The authentication metadata provider can not be null");
            }
            if (StringUtils.isEmpty(clazzName)) {
                return null;
            }
            AuthenticationMetadataProvider result;
            try {
                result = (AuthenticationMetadataProvider) Class.forName(clazzName)
                    .getDeclaredConstructor().newInstance();
                result.initialize(config, metadataService);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load the authentication metadata provider", e);
            }
            return result;
        });
    }

    public static AuthenticationEvaluator getEvaluator(AuthConfig config) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthenticationEvaluator(config));
    }

    public static AuthenticationEvaluator getEvaluator(AuthConfig config, Supplier<?> metadataService) {
        return computeIfAbsent(EVALUATOR_PREFIX + config.getConfigName(), key -> new AuthenticationEvaluator(config, metadataService));
    }

    public static AuthenticationContext newContext(AuthConfig config, Metadata metadata) {
        AuthenticationProvider<AuthenticationContext> authenticationProvider = getProvider(config);
        if (authenticationProvider == null) {
            return null;
        }
        return authenticationProvider.newContext(metadata);
    }

    public static AuthenticationContext newContext(AuthConfig config, RemotingCommand command) {
        AuthenticationProvider<AuthenticationContext> authenticationProvider = getProvider(config);
        if (authenticationProvider == null) {
            return null;
        }
        return authenticationProvider.newContext(command);
    }

    @SuppressWarnings("unchecked")
    private static <V> V computeIfAbsent(String key, Function<String, ? extends V> function) {
        Object value = INSTANCE_MAP.computeIfAbsent(key, function);
        return value != null ? (V) value : null;
    }
}
