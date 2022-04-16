package org.apache.rocketmq.namesrv.controller;

import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;

/**
 * The implementation of controllerApi, based on dledger (raft).
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 14:58
 */
public class DledgerController implements Controller {


    @Override
    public CompletableFuture<AlterInSyncReplicasResponseHeader> alterInSyncReplicas(final AlterInSyncReplicasRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<GetReplicaInfoResponseHeader> getReplicaInfo(final GetRouteInfoRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<GetMetaDataResponseHeader> getMetadata(final GetMetaDataRequestHeader request) {
        return null;
    }
}
