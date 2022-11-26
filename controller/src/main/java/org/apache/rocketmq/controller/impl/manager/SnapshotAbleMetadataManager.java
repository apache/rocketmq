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
package org.apache.rocketmq.controller.impl.manager;

/**
 * The interface of MetadataManager. Subclasses can support snapshot metadata.
 */
public interface SnapshotAbleMetadataManager {

    /**
     * Encode the metadata contained in this MetadataManager.
     * @return encoded metadata
     */
    byte[] encodeMetadata();


    /**
     *
     * According to the param data, load metadata into the MetadataManager.
     * @param data encoded metadata
     * @return true if load metadata success.
     */
    boolean loadMetadata(byte[] data);


    /**
     * Get the type of this MetadataManager.
     * @return MetadataManagerType
     */
    MetadataManagerType getMetadataManagerType();

}
