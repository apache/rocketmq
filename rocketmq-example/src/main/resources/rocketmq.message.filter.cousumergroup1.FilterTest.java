package rocketmq.message.filter.cousumergroup1;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;


public class FilterTest implements MessageFilter {

    @Override
    public boolean match(MessageExt msg) {
        // TODO Auto-generated method stub
        return false;
    }
}
