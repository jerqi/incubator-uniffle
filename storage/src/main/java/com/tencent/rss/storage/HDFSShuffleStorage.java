package com.tencent.rss.storage;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.StatusCode;
import java.util.Iterator;
import java.util.List;

public class HDFSShuffleStorage implements ShuffleStorage {

    public List<ShuffleSegment> write(List<ShuffleBlock> shuffleBlocks) {
        return null;
    }


    public StatusCode init(String msg) {
        // TODO: construct hdfs path and create
        msg = "";
        createPath("");
        return StatusCode.SUCCESS;
    }

    private String createPath(String path) {
        return "";
    }

    public Iterator<ShuffleSegment> next() {
        return null;
    }

    public boolean close() {
        return true;
    }
}
