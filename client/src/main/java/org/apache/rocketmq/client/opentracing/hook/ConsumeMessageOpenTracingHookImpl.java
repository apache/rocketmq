package org.apache.rocketmq.client.opentracing.hook;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.message.MessageExt;

public class ConsumeMessageOpenTracingHookImpl  implements ConsumeMessageHook {
    private Span rootSpan;
    private Span span;
    private Tracer tracer;

    public ConsumeMessageOpenTracingHookImpl(Span rootSpan,Tracer tracer) {
        this.rootSpan = rootSpan;
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageOpenTracingHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        span = tracer.buildSpan("ConsumeMessage").asChildOf(rootSpan).start();

    }


    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {

        for(MessageExt ext:context.getMsgList()){
            span.log(ImmutableMap.of("topic:",ext.getTopic(), "msgId:", ext.getMsgId(),
                "tag:", ext.getTags(), "key:", ext.getKeys(), "bodyLength:", ext.getBody().length));
        }
        span.finish();
    }
}
