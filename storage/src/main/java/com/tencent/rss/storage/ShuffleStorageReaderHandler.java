package com.tencent.rss.storage;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import java.io.IOException;
import java.util.List;

public interface ShuffleStorageReaderHandler<T extends ShuffleSegment> {

    /**
     * Read block from storage
     *
     * @return return null if EOF or 1000 blocks at most
     * @throws IOException
     * @throws IllegalStateException
     */
    List<ShuffleBlock> readData() throws IOException, IllegalStateException;

    /**
     * Read at most limit blocks
     *
     * @param limit maximum blocks to read
     * @return return null if EOF or limit blocks at most
     * @throws IOException
     * @throws IllegalStateException
     */
    List<ShuffleBlock> readData(int limit) throws IOException, IllegalStateException;

    List<T> readIndex() throws IOException, IllegalStateException;

    List<T> readIndex(int limit) throws IOException, IllegalStateException;
}
