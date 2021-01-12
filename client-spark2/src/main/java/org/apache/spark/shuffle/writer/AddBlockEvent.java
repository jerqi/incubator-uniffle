package org.apache.spark.shuffle.writer;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;

public class AddBlockEvent {

  private String taskId;
  private List<ShuffleBlockInfo> shuffleDataInfo;

  public AddBlockEvent(String taskId, List<ShuffleBlockInfo> shuffleDataInfo) {
    this.taskId = taskId;
    this.shuffleDataInfo = shuffleDataInfo;
  }

  public String getTaskId() {
    return taskId;
  }

  public List<ShuffleBlockInfo> getShuffleDataInfo() {
    return shuffleDataInfo;
  }

  @Override
  public String toString() {
    return "AddBlockEvent: TaskId[" + taskId + "], " + shuffleDataInfo;
  }
}
