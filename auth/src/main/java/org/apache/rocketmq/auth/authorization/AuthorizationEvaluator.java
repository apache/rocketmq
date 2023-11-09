package org.apache.rocketmq.auth.authorization;

import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthorizationEvaluator {

    private final AuthorizationProvider authorizationProvider;

    public AuthorizationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthorizationEvaluator(AuthConfig config, Supplier<?> metadataService) {
        this.authorizationProvider = AuthorizationFactory.getProvider(config);
        if (this.authorizationProvider!= null) {
            this.authorizationProvider.initialize(config, metadataService);
        }
    }

    public void evaluate(List<AuthorizationContext> contexts) {
        if (this.authorizationProvider == null) {
            return;
        }
        contexts.forEach(this::evaluate);
    }

    public void evaluate(AuthorizationContext context) {
        if (this.authorizationProvider == null) {
            return;
        }
        try {
            this.authorizationProvider.authorize(context).join();
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Throwable ex) {
            Throwable exception = ExceptionUtils.getRealException(ex);
            if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            } else {
                throw new AuthorizationException("failed to authorization the request", exception);
            }
        }
    }
}
