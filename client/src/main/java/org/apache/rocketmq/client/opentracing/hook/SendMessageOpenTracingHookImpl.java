package org.apache.rocketmq.client.opentracing.hook;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;

public class SendMessageOpenTracingHookImpl   implements SendMessageHook {
    private Span rootSpan;
    private Span span;
    private Tracer tracer;

    public SendMessageOpenTracingHookImpl(Span rootSpan,Tracer tracer){
        this.rootSpan = rootSpan;
        this.tracer = tracer;

    }

    @Override
    public String hookName() {
        return "SendMessageOpenTracingHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        span = tracer.buildSpan("sendMessage").asChildOf(rootSpan).start();

    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        span.log(ImmutableMap.of("topic",context.getSendResult().getMessageQueue().getTopic(),"msgId",context.getSendResult().getMsgId(),
            "offsetMsgId",context.getSendResult().getOffsetMsgId(),"tag",context.getMessage().getTags(),"key",context.getMessage().getKeys()));
        span.log(ImmutableMap.of("msgType",context.getMsgType(),"bodyLength",context.getMessage().getBody().length));
        span.finish();
    }
}
