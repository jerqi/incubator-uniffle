package com.tencent.rss.storage;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import java.io.IOException;
import java.util.List;

public interface ShuffleStorageWriteHandler {

    /**
     * Write the blocks to storage
     *
     * @param shuffleBlocks blocks to storage
     * @throws IOException
     * @throws IllegalStateException
     */
    void write(List<ShuffleBlock> shuffleBlocks) throws IOException, IllegalStateException;
}
