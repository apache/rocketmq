/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

public class Exceptions {

    static class PutMessageException extends Exception {

        private PutMessageStatus errorCode = PutMessageStatus.UNKNOWN_ERROR;
        private AppendMessageResult result = null;

        public PutMessageException() {}

        public PutMessageException(PutMessageStatus errorCode) {
            this.errorCode = errorCode;
        }

        public PutMessageException(AppendMessageResult result) {
            this.result = result;
        }

        public PutMessageException(PutMessageStatus errorCode, AppendMessageResult result) {
            this.result = result;
            this.errorCode = errorCode;
        }

//        public PutMessageStatus errorCode() {
//            return errorCode;
//        }

        public PutMessageResult toPutMessageResult() {
            return new PutMessageResult(this.errorCode, this.result);
        }
    }

    static class CreateMappedfileFailedException extends PutMessageException {

        public CreateMappedfileFailedException() {
            super(PutMessageStatus.CREATE_MAPEDFILE_FAILED);
        }

        public CreateMappedfileFailedException(AppendMessageResult result) {
            super(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
        }
    }

    static class MessageIllegalException extends PutMessageException {

        public MessageIllegalException() {
            super(PutMessageStatus.MESSAGE_ILLEGAL);
        }

        public MessageIllegalException(AppendMessageResult result) {
            super(PutMessageStatus.MESSAGE_ILLEGAL, result);
        }
    }

}
