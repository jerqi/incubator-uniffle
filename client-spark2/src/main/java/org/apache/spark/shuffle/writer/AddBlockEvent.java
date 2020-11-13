package org.apache.spark.shuffle.writer;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import java.util.Map;

public class AddBlockEvent {

    private long taskAttemptId;
    private Map<Integer, ShuffleBlock> shuffleDataInfo;

    public AddBlockEvent(long taskAttemptId,
            Map<Integer, ShuffleBlock> shuffleDataInfo) {
        this.taskAttemptId = taskAttemptId;
        this.shuffleDataInfo = shuffleDataInfo;
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    public Map<Integer, ShuffleBlock> getShuffleDataInfo() {
        return shuffleDataInfo;
    }
}
