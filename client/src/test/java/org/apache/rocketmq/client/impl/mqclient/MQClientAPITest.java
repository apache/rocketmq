package org.apache.rocketmq.client.impl.mqclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MQClientAPITest {

    private NameserverAccessConfig nameserverAccessConfig;
    private final ClientRemotingProcessor clientRemotingProcessor = new DoNothingClientRemotingProcessor(null);
    private final RPCHook rpcHook = null;
    private ScheduledExecutorService scheduledExecutorService;
    private MQClientAPIFactory mqClientAPIFactory;

    @BeforeEach
    void setUp() {
        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("TestScheduledExecutorService", true);
    }

    @AfterEach
    public void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    void testInitWithNamesrvAddr() {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        assertEquals("127.0.0.1:9876", System.getProperty("rocketmq.namesrv.addr"));
    }

    @Test
    void testInitWithNamesrvDomain() {
        nameserverAccessConfig = new NameserverAccessConfig("", "test-domain", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        assertEquals("test-domain", System.getProperty("rocketmq.namesrv.domain"));
    }

    @Test
    void testInitThrowsExceptionWhenBothEmpty() {
        nameserverAccessConfig = new NameserverAccessConfig("", "", "");

        RuntimeException exception = assertThrows(RuntimeException.class, () -> new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        ));

        assertEquals("The configuration item NamesrvAddr is not configured", exception.getMessage());
    }

    @Test
    void testStartCreatesClients() throws Exception {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:123");

        mqClientAPIFactory.start();

        // Assert
        MQClientAPIExt client = mqClientAPIFactory.getClient();
        List<String> nameServerAddressList = client.getNameServerAddressList();
        assertEquals(1, nameServerAddressList.size());
        assertEquals("127.0.0.1:9876", nameServerAddressList.get(0));
    }

    @Test
    void testOnNameServerAddressChangeUpdatesAllClients() throws Exception {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );
        mqClientAPIFactory.start();

        // Act
        mqClientAPIFactory.onNameServerAddressChange("new-address0;new-address1");

        MQClientAPIExt client = mqClientAPIFactory.getClient();
        List<String> nameServerAddressList = client.getNameServerAddressList();
        assertEquals(2, nameServerAddressList.size());
        assertEquals("new-address0", nameServerAddressList.get(0));
    }
}
