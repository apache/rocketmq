package org.apache.rocketmq.remoting;

import java.util.Map;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class RemotingClientFactory {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private RemotingClientFactory() {
    }

    private static Map<String, String> paths;

    private static final String CLIENT_LOCATION = "META-INF/service/org.apache.rocketmq.remoting.RemotingClient";

    static {
        paths = ServiceProvider.loadPath(CLIENT_LOCATION);
    }

    public static RemotingClient createInstance(String protocol) {
        return ServiceProvider.createInstance(paths.get(protocol), RemotingClient.class);
    }

    public static RemotingClient createInstance() {
        return ServiceProvider.createInstance(paths.get(RemotingUtil.DEFAULT_PROTOCOL), RemotingClient.class);
    }
}
