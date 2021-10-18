## RocketMQ gRPC

### Api
This implementation follows the proto of [rocketmq-apis](https://github.com/apache/rocketmq-apis).

```shell
### Submodule
git submodule update --init --recursive
```
### Related configuration
```shell
### Namesrv
### Enable gRPC server
org.apache.rocketmq.common.namesrv.NamesrvConfig#enableGrpcServer
### Broker
### Enable gRPC server
org.apache.rocketmq.common.BrokerConfig#enableGrpcServer
### Enable gRPC transaction. Transaction doesn't support both remoting and gRPC protocol at same time for now
org.apache.rocketmq.common.BrokerConfig#enableGrpcTransaction
### Service Port
org.apache.rocketmq.grpc.server.GrpcServerConfig#port
### Protocol Negotiation
org.apache.rocketmq.remoting.netty.NettyServerConfig#enableHttp2Proxy
org.apache.rocketmq.remoting.netty.NettyServerConfig#enableHttp2SslProxy
org.apache.rocketmq.remoting.netty.NettyServerConfig#http2ProxyHost
org.apache.rocketmq.remoting.netty.NettyServerConfig#inheritGrpcPortToHTTP2
org.apache.rocketmq.remoting.netty.NettyServerConfig#http2ProxyPort
```

### Develop Guide
#### maven
```shell
mvn clean compile -pl grpc
```
#### idea
1. Maven -> rocketmq-grpc -> protobuf -> protobuf:compile & protobuf:compile-custom
2. File -> Project Structure -> Mark *grpc/target/generated-sources/protobuf/grpc-java* and *grpc/target/generated-sources/protobuf/java* as **Sources** 
