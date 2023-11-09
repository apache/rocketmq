package org.apache.rocketmq.auth.authentication;

import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthenticationEvaluator {

    private final AuthenticationProvider<AuthenticationContext> authenticationProvider;

    public AuthenticationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthenticationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        authenticationProvider = AuthenticationFactory.getProvider(authConfig);
        if (authenticationProvider != null) {
            authenticationProvider.initialize(authConfig, metadataService);
        }
    }

    public void evaluate(AuthenticationContext context) {
        if (this.authenticationProvider == null) {
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
            } else {
                throw new AuthenticationException("Failed to authentication the request", exception);
            }
        }
    }
}
