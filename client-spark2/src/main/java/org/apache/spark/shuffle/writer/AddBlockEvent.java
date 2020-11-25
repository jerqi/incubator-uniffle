package org.apache.spark.shuffle.writer;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;

public class AddBlockEvent {

    private String taskIdentify;
    private List<ShuffleBlockInfo> shuffleDataInfo;

    public AddBlockEvent(String taskIdentify, List<ShuffleBlockInfo> shuffleDataInfo) {
        this.taskIdentify = taskIdentify;
        this.shuffleDataInfo = shuffleDataInfo;
    }

    public String getTaskIdentify() {
        return taskIdentify;
    }

    public List<ShuffleBlockInfo> getShuffleDataInfo() {
        return shuffleDataInfo;
    }

}
