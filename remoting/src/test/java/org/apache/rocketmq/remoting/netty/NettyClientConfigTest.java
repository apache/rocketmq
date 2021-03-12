package org.apache.rocketmq.remoting.netty;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class NettyClientConfigTest {

  @Test
  public void testChangeConfigBySystemProperty() throws NoSuchFieldException, IllegalAccessException {

    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "1");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "1023");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "1024");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "2000");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS, "60");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "16383");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "16384");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT, "false");

    NettyClientConfig changedConfig = new NettyClientConfig();
    assertThat(changedConfig.getClientWorkerThreads()).isEqualTo(1);
    assertThat(changedConfig.getClientOnewaySemaphoreValue()).isEqualTo(1023);
    assertThat(changedConfig.getClientAsyncSemaphoreValue()).isEqualTo(1024);
    assertThat(changedConfig.getConnectTimeoutMillis()).isEqualTo(2000);
    assertThat(changedConfig.getClientChannelMaxIdleTimeSeconds()).isEqualTo(60);
    assertThat(changedConfig.getClientSocketSndBufSize()).isEqualTo(16383);
    assertThat(changedConfig.getClientSocketRcvBufSize()).isEqualTo(16384);
    assertThat(changedConfig.isClientCloseSocketIfTimeout()).isEqualTo(false);
  }
}
