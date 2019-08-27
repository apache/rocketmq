package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RequestFutureTable {
    private static InternalLogger log = ClientLogger.getLog();
    private static ConcurrentHashMap<String, RequestResponseFuture> requestFutureTable = new ConcurrentHashMap<String, RequestResponseFuture>();

    public static ConcurrentHashMap<String, RequestResponseFuture> getRequestFutureTable(){
        return requestFutureTable;
    }

    public static void scanExpiredRequest(){
        final List<RequestResponseFuture> rfList = new LinkedList<RequestResponseFuture>();
        Iterator<Map.Entry<String, RequestResponseFuture>> it = requestFutureTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RequestResponseFuture> next = it.next();
            RequestResponseFuture rep = next.getValue();

            if (rep.isTimeout()) {
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, REQUEST_UNIQ_ID={}" + rep.getRequestUniqId());
            }
        }

        for (RequestResponseFuture rf : rfList) {
            try {
                Throwable cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.");
                rf.setCause(cause);
                rf.executeRequestCallback();
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }
}
