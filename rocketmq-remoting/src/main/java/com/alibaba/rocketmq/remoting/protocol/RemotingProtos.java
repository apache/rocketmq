package com.alibaba.rocketmq.remoting.protocol;

public final class RemotingProtos {
    private RemotingProtos() {
    }

    public enum ResponseCode {
        // 成功
        SUCCESS(0, 0),
        // 发生了未捕获异常
        SYSTEM_ERROR(1, 1),
        // 由于线程池拥堵，系统繁忙
        SYSTEM_BUSY(2, 2),
        // 请求代码不支持
        REQUEST_CODE_NOT_SUPPORTED(3, 3), ;

        // /////////////////////////////////////////////////////////////////////

        // 成功
        public static final int SUCCESS_VALUE = 0;
        // 发生了未捕获异常
        public static final int SYSTEM_ERROR_VALUE = 1;
        // 由于线程池拥堵，系统繁忙
        public static final int SYSTEM_BUSY_VALUE = 2;
        // 请求代码不支持
        public static final int REQUEST_CODE_NOT_SUPPORTED_VALUE = 3;


        public static ResponseCode valueOf(int value) {
            switch (value) {
            case 0:
                return SUCCESS;
            case 1:
                return SYSTEM_ERROR;
            case 2:
                return SYSTEM_BUSY;
            case 3:
                return REQUEST_CODE_NOT_SUPPORTED;
            default:
                return null;
            }
        }

        private final int index;
        private final int value;


        public final int getNumber() {
            return value;
        }


        public int getIndex() {
            return index;
        }


        private ResponseCode(int index, int value) {
            this.index = index;
            this.value = value;
        }

    }
}
