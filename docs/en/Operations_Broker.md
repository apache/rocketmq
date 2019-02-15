# 3 Broker

## 3.1 Broker Role
   Broker roles are divided into ASYNC_MASTER, SYNC_MASTER, and SLAVE. 
If the reliability requirements of the message are strict, you can use the SYNC_MASTER plus SLAVE deployment method. 
If the reliability of the message is not high, you can use ASYNC_MASTER plus SLAVE deployment. 
If it is only convenient to test, you can choose to deploy only ASYNC_MASTER or SYNC_MASTER only.
## 3.2 FlushDiskType
  SYNC_FLUSH loses a lot of performance compared to ASYNC_FLUSH,
but it is also more reliable, so you need to make trade-offs based on actual business scenarios.
## 3.3 Broker Configuration
| Parameter name                           | Default                        | Description                                                         |
| -------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| listenPort                    | 10911              | listen port for client |
| namesrvAddr       | null                         | name server address     |
| brokerIP1 | InetAddress for network interface                         | Should be configured if having multiple addresses |
| brokerIP2 | InetAddress for network interface                         | If configured for the Master broker in the Master/Slave cluster, slave broker will connect to this port for data synchronization   |
| brokerName        | null                         | broker name                           |
| brokerClusterName                     | DefaultCluster                  | this broker belongs to which cluster           |
| brokerId             | 0                              | broker id, 0 means master, positive integers mean slave                                                 |
| storePathCommitLog                      | $HOME/store/commitlog/                              | file path for commit log                                                 |
| storePathConsumerQueue                   | $HOME/store/consumequeue/                              | file path for consume queue                                              |
| mapedFileSizeCommitLog     | 1024 * 1024 * 1024(1G) | mapped file size for commit log                                        |​ 
| deleteWhen     | 04 | When to delete the commitlog which is out of the reserve time                                        |​ 
| fileReserverdTime     | 72 | The number of hours to keep a commitlog before deleting it                                        |​ 
| brokerRole     | ASYNC_MASTER | SYNC_MASTER/ASYNC_MASTER/SLAVE                                        |​ 
| flushDiskType     | ASYNC_FLUSH | {SYNC_FLUSH/ASYNC_FLUSH}. Broker of SYNC_FLUSH mode flushes each message onto disk before acknowledging producer. Broker of ASYNC_FLUSH mode, on the other hand, takes advantage of group-committing, achieving better performance.                                        |​