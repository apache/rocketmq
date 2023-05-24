# Dledger cluster deployment
---
## preface
This document introduces how to deploy auto failover RocketMQ-on-DLedger Group.

RocketMQ-on-DLedger Group is a broker group with **same name**, needs at least 3 nodes, elect a Leader by Raft algorithm automatically, the others as Follower, replicating data between Leader and Follower for system high available.  
RocketMQ-on-DLedger Group can failover automatically, and maintains consistent.  
RocketMQ-on-DLedger Group can scale up horizontal, that is, can deploy any RocketMQ-on-DLedger Groups providing services external.  

## 1. New cluster deployment

#### 1.1 Write the configuration
each RocketMQ-on-DLedger Group needs at least 3 machines.(assuming 3 in this document)  
write 3 configuration files, advising refer to the directory of conf/dledger 's example configuration file.  
key configuration items:  

| name | meaning | example |
| --- | --- | --- |
| enableDLegerCommitLog | whether enable DLedger  | true |
| dLegerGroup | DLedger Raft Group's name, advising maintain consistent to brokerName | RaftNode00 |
| dLegerPeers | DLedger Group's nodes port infos, each node's configuration stay consistent in the same group. | n0-127.0.0.1:40911;n1-127.0.0.1:40912;n2-127.0.0.1:40913 |
| dLegerSelfId | node id, must belongs to dLegerPeers; each node is unique in the same group. | n0 |
| sendMessageThreadPoolNums | the count of sending thread, advising set equal to the cpu cores. | 16 |

the following presents an example configuration conf/dledger/broker-n0.conf.  

```
brokerClusterName = RaftCluster
brokerName=RaftNode00
listenPort=30911
namesrvAddr=127.0.0.1:9876
storePathRootDir=/tmp/rmqstore/node00
storePathCommitLog=/tmp/rmqstore/node00/commitlog
enableDLegerCommitLog=true
dLegerGroup=RaftNode00
dLegerPeers=n0-127.0.0.1:40911;n1-127.0.0.1:40912;n2-127.0.0.1:40913
## must be unique
dLegerSelfId=n0
sendMessageThreadPoolNums=16
```

### 1.2 Start Broker

Startup stays consistent with the old version.

`nohup sh bin/mqbroker -c conf/dledger/xxx-n0.conf & `  
`nohup sh bin/mqbroker -c conf/dledger/xxx-n1.conf & `  
`nohup sh bin/mqbroker -c conf/dledger/xxx-n2.conf & `  


## 2. Upgrade old cluster

If old cluster deployed in Master mode, then each Master needs to be transformed into a RocketMQ-on-DLedger Group.  
If old cluster deployed in Master-Slave mode, then each Master-Slave group needs to be transformed into a RocketMQ-on-DLedger Group.

### 2.1 Kill old Broker

execute kill command, or call `bin/mqshutdown broker`.

### 2.2 Check old Commitlog

Each node in RocketMQ-on-DLedger group is compatible with old Commitlog, but Raft replicating process works on the adding message only. So, to avoid occurring exceptions, old Commitlog must be consistent.
If old cluster deployed in Master-Slave mode, it maybe inconsistent after shutdown. Advising use md5sum to check at least 2 recently Commitlog file, if occur inconsistent, maintain consistent by copy.

Although RocketMQ-on-DLedger Group can deployed with 2 nodes, it lacks failover ability(at least 3 nodes can tolerate one node fail).
Make sure that both Master and Slave's Commitlog is consistent, then prepare 3 machines, copy old Commitlog from Master to this 3 machines(BTW, copy the config directory).
   
Then, go ahead to set configurations.

### 2.3 Modify configuration

Refer to New cluster deployment.

### 2.4 Restart Broker 

Refer to New cluster deployment.


