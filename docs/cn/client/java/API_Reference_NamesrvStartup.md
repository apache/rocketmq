##NamesrvStarup
---
###类简介
‘public class NamesrvStartup’

>`NamesrvStartup`类是namesrv启动的入口类，在本地配置好环境变量ROCKETMQ_HOME后可通过执行其中的main方法来快速启动一个namesrv。namesrv主要负责维护活跃broker的信息和topic以及topic所在队列的信息。其中启动参数主要由namesrvConfig和nettyServerConfig这两个类来初始化，这两个类后面讲到，当然这些配置也可以通过启动指定配置文件进行修改。

``` java
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            //NamesrvController是namesrv接受请求并响应的核心类,
            NamesrvController controller = createNamesrvController(args);
            //启动并初始化NamesrvController
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    } 
```

###namesrv启动过程一：创建NamesrvController
createNamesrvController中会创建并初始化namesrvConfig和nettyServerConfig的实例

#### namesrvConfig参数说明
|类型|参数名称|描述|
|------|-------|-------|
|String|rocketmqHome|rocketmq的主目录|
|String|kvConfigPath|kv配置属性的持久化路径|
|String|configStorePath|namesrv的配置文件路径，可以使用-c指定|
|String|productEnvName|环境名|
|boolean|clusterTest|是否开启集群测试，默认false|
|boolean|orderMessageEnable|是否支持顺序消息，默认是false|

#### nettyServerConfig参数说明
|类型|参数名称|描述|
|------|-------|-------|
|int|listenPort|nameServer监听的端口，是8888，后面会被初始化为9876|
|int|serverWorkerThreads|netty的线程池线程个数|
|int|serverCallbackExecutorThreads|netty的公共任务（发送，消费，心跳检测）的线程池线程个数|
|int|serverSelectorThreads|io任务的线程池线程个数|
|int|serverOnewaySemaphoreValue|发送oneway消息时的请求并发度|
|int|serverAsyncSemaphoreValue|异步消息最大并发度|
|int|serverChannelMaxIdleTimeSeconds|网络最大空闲时间|
|int|serverSocketSndBufSize|socket发送端缓冲区大小|
|int|serverSocketRcvBufSize|socket接收端缓存区大小|
|boolean|serverPooledByteBufAllocatorEnable|是否启用bytebuffer缓存（提高读写速度用的）,默认true|
|boolean|useEpollNativeSelector|是否启用Epoll，默认false|

namesrvConfig和nettyServerConfig里的这些配置都可以通过"-c"来指定属性配置文件的位置来重新修改;也可以通过"-p 属性名=属性值"这样来重新指定

最后调用NamesrvController的构造方法创建NamesrvController实例，NamesrvController创建完成

###namesrv启动过程二：初始化NamesrvController

1.调用start方法启动NamesrvController

2.调用NamesrvController的initialize来初始化

3.初始化过程如下：
- 加载kv配置，存在一个kv配置管理器
- 创建netty服务的对象，同时在onChannelDestroy方法规定了当broker宕机时要触发的动作，其实就是把registerBroker方法做的事回退掉
- 加载kv配置，存在一个kv配置管理器
- 创建了一个定时线程池，调用scanNotActiveBroker方法每隔10s来检查哪些broker宕机了
- 创建一个定时线程池，调用printAllPeriodically方法每隔10秒来刷新kv配置
- FileWatchService的作用应该是监听配置文件的改变，有变化有重新加载下
###namesrv启动过程三：释放资源
在执行controller.start()先去注册了一个hook方法，使得在jvm退出（就是namesrv停止前）前先去shutdown这个controller（释放掉之前启的各种线程池）

