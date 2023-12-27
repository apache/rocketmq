package org.apache.rocketmq.auth.authentication;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthenticationEvaluator {

    private final AuthConfig authConfig;
    private final List<String> authenticationWhitelist = new ArrayList<>();
    private final AuthenticationProvider<AuthenticationContext> authenticationProvider;

    public AuthenticationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthenticationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        this.authenticationProvider = AuthenticationFactory.getProvider(authConfig);
        if (this.authenticationProvider != null) {
            this.authenticationProvider.initialize(authConfig, metadataService);
        }
        if (StringUtils.isNotBlank(authConfig.getAuthenticationWhitelist())) {
            String[] whitelist = StringUtils.split(authConfig.getAuthenticationWhitelist(), ",");
            for (String rpcCode : whitelist) {
                this.authenticationWhitelist.add(StringUtils.trim(rpcCode));
            }
        }
    }

    public void evaluate(AuthenticationContext context) {
        if (context == null) {
            return;
        }
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        if (this.authenticationProvider == null) {
            return;
        }
        if (this.authenticationWhitelist.contains(context.getRpcCode())) {
            return;
        }
        try {
            this.authenticationProvider.authenticate(context).join();
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            Throwable exception = ExceptionUtils.getRealException(ex);
            if (exception instanceof AuthenticationException) {
                throw (AuthenticationException) exception;
            }
            throw new AuthenticationException("Failed to authentication the request", exception);
        }
    }
}
