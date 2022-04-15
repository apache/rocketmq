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
 * The api for controller
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 14:57
 */
public interface ControllerApi {

    /**
     * Alter ISR of broker replicas.
     * @param request AlterInSyncReplicasRequest
     * @return AlterInSyncReplicasResponse
     */
    CompletableFuture<AlterInSyncReplicasResponseHeader> alterInSyncReplicas(final AlterInSyncReplicasRequestHeader request);

    /**
     * Elect new master for a broker.
     * @param request ElectMasterRequest
     * @return ElectMasterResponse
     */
    CompletableFuture<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request);


    /**
     * Get the Replica Info for a target broker.
     * @param request GetRouteInfoRequest
     * @return GetReplicaInfoResponse
     */
    CompletableFuture<GetReplicaInfoResponseHeader> getReplicaInfo(final GetRouteInfoRequestHeader request);


    /**
     * Get Metadata of controller
     * @param request GetMetaDataRequest
     * @return GetMetaDataResponse
     */
    CompletableFuture<GetMetaDataResponseHeader> getMetadata(final GetMetaDataRequestHeader request);
}
