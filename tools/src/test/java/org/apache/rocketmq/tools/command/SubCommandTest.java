package org.apache.rocketmq.tools.command;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;

public class SubCommandTest {

    public DefaultMQAdminExt defaultMQAdminExt;

    public RPCHook rpcHook;

    @Before
    public void init() throws Exception {
        rpcHook = new AclClientRPCHook(new SessionCredentials());
        defaultMQAdminExt = PowerMockito.mock(DefaultMQAdminExt.class);
        PowerMockito.whenNew(DefaultMQAdminExt.class).withArguments(rpcHook).thenReturn(defaultMQAdminExt);
    }
}
