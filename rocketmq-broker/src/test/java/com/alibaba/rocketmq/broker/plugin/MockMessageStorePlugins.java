/**
 * 
 */
package com.alibaba.rocketmq.broker.plugin;

import com.alibaba.rocketmq.store.MessageStore;

/**
 * @author qinan.qn@taobao.com
 *2015Äê12ÔÂ12ÈÕ
 */
class MockMessageStorePlugin1 extends AbstractPluginMessageStore{

    /**
     * @param context
     * @param next
     */
    public MockMessageStorePlugin1(MessageStorePluginContext context, MessageStore next) {
        super(context, next);
    }
}
class MockMessageStorePlugin2 extends AbstractPluginMessageStore{

    /**
     * @param context
     * @param next
     */
    public MockMessageStorePlugin2(MessageStorePluginContext context, MessageStore next) {
        super(context, next);
        // TODO Auto-generated constructor stub
    }
}

