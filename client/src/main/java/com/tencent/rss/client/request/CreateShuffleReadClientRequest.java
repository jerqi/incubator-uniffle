package com.tencent.rss.client.request;

import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class CreateShuffleReadClientRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private String storageType;
  private String basePath;
  private Configuration hadoopConf;
  private int indexReadLimit;
  private int readBufferSize;
  private int partitionNumPerRange;
  private int partitionNum;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private List<ShuffleServerInfo> shuffleServerInfoList;

  public CreateShuffleReadClientRequest(String appId, int shuffleId, int partitionId, String storageType,
      String basePath, Configuration hadoopConf, int indexReadLimit, int readBufferSize, int partitionNumPerRange,
      int partitionNum, Roaring64NavigableMap blockIdBitmap, Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.storageType = storageType;
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.indexReadLimit = indexReadLimit;
    this.readBufferSize = readBufferSize;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.blockIdBitmap = blockIdBitmap;
    this.taskIdBitmap = taskIdBitmap;
    this.shuffleServerInfoList = shuffleServerInfoList;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public String getStorageType() {
    return storageType;
  }

  public String getBasePath() {
    return basePath;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public int getIndexReadLimit() {
    return indexReadLimit;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public Roaring64NavigableMap getBlockIdBitmap() {
    return blockIdBitmap;
  }

  public Roaring64NavigableMap getTaskIdBitmap() {
    return taskIdBitmap;
  }

  public List<ShuffleServerInfo> getShuffleServerInfoList() {
    return shuffleServerInfoList;
  }
}
