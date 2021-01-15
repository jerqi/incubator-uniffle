package org.apache.spark.shuffle.writer;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;

public class AddBlockEvent {

  private String taskId;
  private List<ShuffleBlockInfo> shuffleDataInfoList;

  public AddBlockEvent(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
    this.taskId = taskId;
    this.shuffleDataInfoList = shuffleDataInfoList;
  }

  public String getTaskId() {
    return taskId;
  }

  public List<ShuffleBlockInfo> getShuffleDataInfoList() {
    return shuffleDataInfoList;
  }

  @Override
  public String toString() {
    return "AddBlockEvent: TaskId[" + taskId + "], " + shuffleDataInfoList;
  }
}
