# Operation and maintenance management

## Cluster building

### 1 Single Master mode

This is the simplest, but also the riskiest, mode that makes the entire service unavailable once the Broker restarts or goes down.Online environments are not recommended, but can be used for local testing and development.Here are the steps to build。

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

We can see 'The Name Server boot success.. ' in namesrv.log That indicates that the start NameServer has been successful.

**2）Start Broker**

```shell
### Also start Broker first
$ nohup sh bin/mqbroker -n localhost:9876 &

### Then verify that the Broker is started successfully, for example, the IP of Broker is 192.168.1.2 and the name is broker-a
$ tail -f ~/logs/rocketmqlogs/Broker.log 
The broker[broker-a, 192.169.1.2:10911] boot success...
```

We can see 'The broker[brokerName,ip:port] boot success.. ' in Broker.log That indicates that the start Broker has been successful.

### 2 Multiple Master mode

The advantages and disadvantages of a cluster without Slave, being all Master, such as 2 Master or 3 Master, are as follows:

- Advantages: simple configuration and a single Master outage or restart maintenance in the cluster has no impact on the application.When the disk is configured as RAID10, messages are not lost because the RAID10 disk is very reliable, even if the machine is not recoverable (In the case of asynchronous brush disk mode of the message, a small number of messages are lost;If the brush mode of a message is synchronous, a single message is not lost).In this mode, the performance is the highest.
- Disadvantages:during a single machine outage, messages that are not consumed on this machine are not subscribed to until the machine recovers, and message real-time is affected.

The starting steps for multiple Master mode are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master, on machine A assumes that the IP for configuring NameServer is: 192.168.1.1.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-a.properties &
 
### Then start the second Master, on machine B, which also configures NameServer's IP: 192.168.1.1.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-b.properties &

...
```

The boot command shown above is used in the case of a single NameServer.For clusters of multiple NameServer, the address list after the -n argument in the Broker boot command is separated by semicolons, for example, 192.168.1.1: 9876;192.161.2: 9876.

### 3 Multiple Master And Multiple Slave Mode-Asynchronous replication

Each Master configures a Slave, with multiple pairs of Master-Slave.HA uses asynchronous replication, with a short message delay (millisecond) between Master and Slave.The advantages and disadvantages of this mode are as follows:

- Advantages: even if the disk is corrupted, very few messages will be lost and the real-time performance of the message will not be affected;At the same time, when Master is down, consumers can still consume messages from Slave, and the process is transparent to the application itself and does not require human intervention;Performance is almost as high as multiple Master mode;
- Disadvantages:a small number of messages will be lost when Master is down or the disk is corrupted.

The starting steps for multiple Master and multiple Slave mode are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master, on machine A assumes that the IP for configuring NameServer is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a.properties &
 
### Then start the second Master, on machine B, which also configures NameServer's IP: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b.properties &
 
### Then start the first Slave,on machine C assumes that the IP for configuring NameServer is:192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a-s.properties &
 
### Last start the second Slave,on machine D assumes that the IP for configuring NameServer is:192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b-s.properties &
```

The above shows a startup command for 2M-2S-Async mode, similar to other nM-nS-Async modes.

### 4 Multiple Master And Multiple Slave Mode-Synchronous dual write

In this mode, one Slave is also configured for each Master and there are multiple pairs of Master-Slave.HA uses synchronous double-write, that is, only Master and Slaver write successfully to return success to the application.

The advantages and disadvantages of this model are as follows:

- Advantages: neither the data nor the service has a single point of failure. In the case of Master downtime, the message is also undelayed. Service availability and data availability are very high;
- Disadvantages:the performance in this mode is slightly lower than in asynchronous replication mode (about 10% lower), the RT sending a single message is slightly higher, and the current version cannot automatically switch to the Master after the primary node is down.

The starting steps are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master, on machine A assumes that the IP for configuring NameServer is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a.properties &
 
### Then start the second Master, on machine B, which also configures NameServer's IP: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b.properties &
 
### Then start the first Slave,on machine C assumes that the IP for configuring NameServer is:192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a-s.properties &
 
### Last start the second Slave,on machine D assumes that the IP for configuring NameServer is:192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b-s.properties &
```

The above Master and Slave pairing is paired by specifying the same BrokerName parameter, the BrokerId of the Master must be 0, and the BrokerId of the Slave must be greater than 0.