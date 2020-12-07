package com.tencent.rss.storage;

import java.io.IOException;
import java.util.List;

public interface ShuffleStorageReaderHandler<T extends ShuffleSegment> {

    /**
     * Use shuffle segment (aka index) to read data from data file.
     *
     * @param shuffleSegment index of the data
     * @return shuffle data
     * @throws IOException
     * @throws IllegalStateException
     */
    byte[] readData(T shuffleSegment) throws IOException, IllegalStateException;

    /**
     * Read shuffle segments from index file.
     *
     * @return shuffle segments
     * @throws IOException
     * @throws IllegalStateException
     */
    List<T> readIndex() throws IOException, IllegalStateException;

    /**
     * Read limit shuffle segments from index file.
     *
     * @param limit the maximum shuffle segments to read once.
     * @return shuffle segments
     * @throws IOException
     * @throws IllegalStateException
     */
    List<T> readIndex(int limit) throws IOException, IllegalStateException;
}
