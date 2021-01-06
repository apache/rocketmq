package org.apache.rocketmq.client.trace.hook;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

public class PullConsumeMessageTraceHookImpl implements ConsumeMessageHook {

    private TraceDispatcher localDispatcher;

    public PullConsumeMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "PullConsumeMessageTraceHookImpl";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {

    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        TraceContext traceContext = new TraceContext();
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.PullAfter);
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getConsumerGroup()));
        List<TraceBean> beans = new ArrayList<TraceBean>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);

            if (traceOn != null && traceOn.equals("false")) {
                // If trace switch is false ,skip it
                continue;
            }
            TraceBean traceBean = new TraceBean();
            traceBean.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic()));//
            traceBean.setMsgId(msg.getMsgId());//
            traceBean.setTags(msg.getTags());//
            traceBean.setKeys(msg.getKeys());//
            traceBean.setStoreTime(msg.getStoreTimestamp());//
            traceBean.setBodyLength(msg.getStoreSize());//
            traceBean.setRetryTimes(msg.getReconsumeTimes());//
            traceBean.setClientHost(null);
            traceContext.setRegionId(regionId);//
            String contextType = context.getProps().get(MixAll.CONSUME_CONTEXT_TYPE);
            if (contextType != null) {
                traceContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
            }
            beans.add(traceBean);
        }
        if (beans.size() > 0) {
            traceContext.setTraceBeans(beans);
            traceContext.setTimeStamp(System.currentTimeMillis());
            localDispatcher.append(traceContext);
        }
    }
}
