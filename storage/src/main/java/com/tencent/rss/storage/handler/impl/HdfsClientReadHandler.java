package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsClientReadHandler extends AbstractHdfsClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsClientReadHandler.class);

  public HdfsClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Configuration hadoopConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    this.hadoopConf = hadoopConf;
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(appId,
            shuffleId, partitionId, partitionNumPerRange, partitionNum));
    init(fullShufflePath);
    readAllIndexSegments();
  }

}
