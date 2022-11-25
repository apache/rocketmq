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
