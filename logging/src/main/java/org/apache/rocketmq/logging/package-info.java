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

package org.apache.rocketmq.logging;

/*
 This package is a minimal logger on the basis of Apache Log4j without
 file configuration and pattern layout configuration. Main forked files are
 followed as below:
 1. LoggingEvent
 2. Logger
 3. Layout
 4. Level
 5. AsyncAppender
 6. FileAppender
 7. RollingFileAppender
 8. DailyRollingFileAppender
 9. ConsoleAppender

 For more information about Apache Log4j, please go to https://github.com/apache/log4j.
 */