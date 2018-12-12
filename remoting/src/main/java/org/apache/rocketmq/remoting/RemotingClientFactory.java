package org.apache.rocketmq.remoting;

import java.util.Map;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.util.RemotingUtil;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class RemotingClientFactory {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private RemotingClientFactory() {
    }

    private static Map<String, RemotingClient> clients;

    private static final String CLIENT_LOCATION = "META-INF/service/org.apache.rocketmq.remoting.RemotingClient";

    static {
        log.info("begin load client");
        clients = ServiceProvider.load(CLIENT_LOCATION, RemotingClient.class);
        log.info("end load client, size:{}", clients.size());
    }

    public static RemotingClient getClient(String protocolType) {
        return clients.get(protocolType);
    }

    public static RemotingClient getClient() {
        return clients.get(RemotingUtil.DEFAULT_PROTOCOL);
    }
}
