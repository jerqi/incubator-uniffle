package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.common.util.ByteUnit;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.common.ShuffleFileInfo;
import com.tencent.rss.storage.common.ShuffleInfo;
import com.tencent.rss.storage.factory.ShuffleUploadHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.request.CreateShuffleUploadHandlerRequest;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ShuffleUploader contains force mode and normal mode, which is decided by the remain
 * space of the disk, and it can be retrieved from the metadata. In force mode, shuffle
 * data will be upload to remote storage whether the shuffle is finished or not.
 * In normal mode, shuffle data will be upload only when it is finished and the shuffle
 * files Both mode will leave the uploaded data to be delete by the cleaner.
 *
 * The underlying handle to upload files to remote storage support HDFS at present, but
 * it will support other storage type (eg, COS, OZONE) and will add more optional parameters,
 * so ShuffleUploader use Joshua Bloch builder pattern to construct and validate the parameters.
 *
 */
public class ShuffleUploader {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleUploader.class);

  private final DiskItem diskItem;
  private final int uploadThreadNum;
  private final long uploadIntervalMS;
  private final long uploadCombineThresholdMB;
  private final long referenceUploadSpeedMBS;
  private final StorageType remoteStorageType;
  private final String hdfsBathPath;
  private final String serverId;
  private final Configuration hadoopConf;
  private final Thread daemonThread;

  private final ExecutorService executorService;
  private volatile boolean isStopped;

  public ShuffleUploader(Builder builder) {
    this.diskItem = builder.diskItem;
    this.uploadThreadNum = builder.uploadThreadNum;
    this.uploadIntervalMS = builder.uploadIntervalMS;
    this.uploadCombineThresholdMB = builder.uploadCombineThresholdMB;
    this.referenceUploadSpeedMBS = builder.referenceUploadSpeedMBS;
    this.remoteStorageType = builder.remoteStorageType;
    // HDFS related parameters
    this.hdfsBathPath = builder.hdfsBathPath;
    this.serverId = builder.serverId;
    this.hadoopConf = builder.hadoopConf;

    Runnable runnable = () -> {
      run();
    };

    daemonThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(diskItem.getBasePath() + " - ShuffleUploader-%d")
        .build()
        .newThread(runnable);

    executorService = Executors.newFixedThreadPool(
        uploadThreadNum,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(diskItem.getBasePath() + " ShuffleUploadWorker-%d")
            .build());
  }

  public static class Builder {
    private DiskItem diskItem;
    private int uploadThreadNum;
    private long uploadIntervalMS;
    private long uploadCombineThresholdMB;
    private long referenceUploadSpeedMBS;
    private StorageType remoteStorageType;
    private String hdfsBathPath;
    private String serverId;
    private Configuration hadoopConf;

    public Builder() {
      // use HDFS and not force upload by default
      this.remoteStorageType = StorageType.HDFS;
    }

    public Builder diskItem(DiskItem diskItem) {
      this.diskItem = diskItem;
      return this;
    }

    public Builder uploadThreadNum(int uploadThreadNum) {
      this.uploadThreadNum = uploadThreadNum;
      return this;
    }

    public Builder uploadIntervalMS(long uploadIntervalMS) {
      this.uploadIntervalMS = uploadIntervalMS;
      return this;
    }

    public Builder uploadCombineThresholdMB(long uploadCombineThresholdMB) {
      this.uploadCombineThresholdMB = uploadCombineThresholdMB;
      return this;
    }

    public Builder referenceUploadSpeedMBS(long referenceUploadSpeedMBS) {
      this.referenceUploadSpeedMBS = referenceUploadSpeedMBS;
      return this;
    }

    public Builder remoteStorageType(StorageType remoteStorageType) {
      this.remoteStorageType = remoteStorageType;
      return this;
    }

    public Builder hdfsBathPath(String hdfsBathPath) {
      this.hdfsBathPath = hdfsBathPath;
      return this;
    }

    public Builder serverId(String serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder hadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    public ShuffleUploader build() throws IllegalArgumentException {
      validate();
      return new ShuffleUploader(this);
    }

    private void validate() throws IllegalArgumentException {
      // check common parameters
      if (diskItem == null) {
        throw new IllegalArgumentException("Disk item is not set");
      }

      if (uploadThreadNum <= 0) {
        throw new IllegalArgumentException("Upload thread num must > 0");
      }

      if (uploadIntervalMS <= 0) {
        throw new IllegalArgumentException("Upload interval must > 0");
      }

      if (uploadCombineThresholdMB <= 0) {
        throw new IllegalArgumentException("Upload combine threshold num must > 0");
      }

      if (referenceUploadSpeedMBS <= 0) {
        throw new IllegalArgumentException("Upload reference speed must > 0");
      }

      // check remote storage related parameters
      if (remoteStorageType == StorageType.HDFS) {
        if (StringUtils.isEmpty(hdfsBathPath)) {
          throw new IllegalArgumentException("HDFS base path is not set");
        }

        if (StringUtils.isEmpty(serverId)) {
          throw new IllegalArgumentException("Server id of file prefix is not set");
        }

        if (hadoopConf == null) {
          throw new IllegalArgumentException("HDFS configuration is not set");
        }

      } else {
        throw new IllegalArgumentException(remoteStorageType + " remote storage type is not supported!");
      }
    }
  }

  public void run() {
    while (!isStopped) {
      try {
        long start = System.currentTimeMillis();
        upload();
        long uploadTime = System.currentTimeMillis() - start;
        LOG.info("{} upload use {}ms", Thread.currentThread().getName(), uploadTime);

        if (uploadTime < uploadIntervalMS) {
          Uninterruptibles.sleepUninterruptibly(uploadIntervalMS - uploadTime, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        LOG.error("{} - upload exception: {}", Thread.currentThread().getName(), ExceptionUtils.getStackTrace(e));
      }
    }
  }

  public void start() {
    daemonThread.start();
  }

  public void stop() {
    isStopped = true;
    Uninterruptibles.joinUninterruptibly(daemonThread);
    executorService.shutdownNow();
  }

  // upload is a blocked until uploading success or timeout exception
  private void upload() {

    boolean forceUpload = !diskItem.canWrite();

    List<ShuffleFileInfo> shuffleFileInfos = selectShuffleFiles(uploadThreadNum, forceUpload);
    if (shuffleFileInfos == null || shuffleFileInfos.isEmpty()) {
      return;
    }

    List<Callable<ShuffleUploadResult>> callableList = Lists.newLinkedList();
    long maxSize = 0;
    for (ShuffleFileInfo shuffleFileInfo : shuffleFileInfos) {
      if (!shuffleFileInfo.isValid()) {
        continue;
      }
      ReadWriteLock lock = diskItem.getLock(shuffleFileInfo.getKey());
      if (forceUpload) {
        boolean locked = lock.writeLock().tryLock();
        if (!locked) {
          continue;
        }
      }
      maxSize = Math.max(maxSize, shuffleFileInfo.getSize());
      Callable<ShuffleUploadResult> callable = () -> {
        try {
          CreateShuffleUploadHandlerRequest request =
              new CreateShuffleUploadHandlerRequest.Builder()
                  .remoteStorageType(remoteStorageType)
                  .remoteStorageBasePath(
                      ShuffleStorageUtils.getFullShuffleDataFolder(hdfsBathPath, shuffleFileInfo.getKey()))
                  .hadoopConf(hadoopConf)
                  .hdfsFilePrefix(serverId)
                  .combineUpload(shuffleFileInfo.shouldCombine(uploadCombineThresholdMB))
                  .build();

          ShuffleUploadHandler handler = ShuffleUploadHandlerFactory.getInstance().createShuffleUploadHandler(request);
          ShuffleUploadResult shuffleUploadResult = handler.upload(
              shuffleFileInfo.getDataFiles(),
              shuffleFileInfo.getIndexFiles(),
              shuffleFileInfo.getPartitions());
          if (shuffleUploadResult == null) {
            return null;
          }
          shuffleUploadResult.setShuffleKey(shuffleFileInfo.getKey());
          if (forceUpload) {
            for (File file : shuffleFileInfo.getDataFiles()) {
              file.delete();
            }
            for (File file : shuffleFileInfo.getIndexFiles()) {
              file.delete();
            }
            diskItem.removeShuffle(shuffleUploadResult.getShuffleKey(), shuffleUploadResult.getSize(),
                shuffleUploadResult.getPartitions());
          }
          return shuffleUploadResult;
        } catch (Exception e) {
          LOG.error("Fail to construct upload callable list {}", ExceptionUtils.getStackTrace(e));
          return null;
        } finally {
          if (forceUpload) {
            lock.writeLock().unlock();
          }
        }
      };
      callableList.add(callable);
    }

    long uploadTimeoutS = calculateUploadTime(maxSize);
    LOG.info("Start to upload {} shuffle info and timeout is {} Seconds", callableList.size(), uploadTimeoutS);
    try {
      List<Future<ShuffleUploadResult>> futures =
          executorService.invokeAll(callableList, uploadTimeoutS, TimeUnit.SECONDS);
      for (Future<ShuffleUploadResult> future : futures) {
        if (future.isDone()) {
          ShuffleUploadResult shuffleUploadResult = future.get();
          if (shuffleUploadResult == null || forceUpload) {
            continue;
          }
          String shuffleKey = shuffleUploadResult.getShuffleKey();
          diskItem.updateUploadedShuffle(shuffleKey, shuffleUploadResult.getSize(),
              shuffleUploadResult.getPartitions());
        } else {
          future.cancel(true);
        }
      }

    } catch (Exception e) {
      LOG.error(
          "Fail to upload {}, {}",
          shuffleFileInfos.stream().map(ShuffleFileInfo::getKey).collect(Collectors.joining("\n")),
          ExceptionUtils.getStackTrace(e));
    }
  }

  @VisibleForTesting
  long calculateUploadTime(long size) {
    long uploadTimeoutS = 1L;
    long cur = ByteUnit.BYTE.toMiB(size) / referenceUploadSpeedMBS;
    if (cur <= uploadTimeoutS) {
      return uploadTimeoutS * 2;
    } else {
      return cur * 2;
    }
  }

  @VisibleForTesting
  List<ShuffleFileInfo> selectShuffleFiles(int num, boolean forceUpload) {
    List<ShuffleFileInfo> shuffleFileInfoList = Lists.newLinkedList();
    List<String> shuffleKeys = diskItem.getSortedShuffleKeys(!forceUpload, num);
    if (shuffleKeys.isEmpty()) {
      return Lists.newArrayList();
    }

    LOG.info("Get {} candidate shuffles {}", shuffleKeys.size(), shuffleKeys);
    for (String shuffleKey : shuffleKeys) {
      List<Integer> partitions = getNotUploadedPartitions(shuffleKey);
      long sz = getNotUploadedSize(shuffleKey);
      if (partitions.isEmpty() || sz <= 0) {
        LOG.warn("{} size {} partitions {}", shuffleKey, sz, partitions);
        continue;
      }

      ShuffleInfo shuffleInfo = new ShuffleInfo(shuffleKey, sz);
      ShuffleFileInfo shuffleFileInfo = generateShuffleFileInfos(shuffleInfo, partitions);
      if (!shuffleFileInfo.isEmpty()) {
        LOG.info(
            "Add shuffle file info key is {} partitions are {}",
            shuffleFileInfo.getKey(), shuffleFileInfo.getPartitions());
        shuffleFileInfoList.add(shuffleFileInfo);
      }
    }
    return shuffleFileInfoList;
  }

  private List getNotUploadedPartitions(String key) {
    RoaringBitmap bitmap = diskItem.getNotUploadedPartitions(key);
    List<Integer> partitionList = Lists.newArrayList();
    for (int p : bitmap) {
      partitionList.add(p);
    }
    partitionList.sort(Integer::compare);
    return partitionList;
  }

  private long getNotUploadedSize(String key) {
    return diskItem.getNotUploadedSize(key);
  }

  private ShuffleFileInfo generateShuffleFileInfos(ShuffleInfo shuffleInfo, List<Integer> partitions) {
    ShuffleFileInfo shuffleFileInfo = new ShuffleFileInfo();
    for (int partition : partitions) {
      String filePrefix = ShuffleStorageUtils.generateAbsoluteFilePrefix(
          diskItem.getBasePath(), shuffleInfo.getKey(), partition, serverId);
      String dataFileName = ShuffleStorageUtils.generateDataFileName(filePrefix);
      String indexFileName = ShuffleStorageUtils.generateIndexFileName(filePrefix);

      File dataFile = new File(dataFileName);
      if (!dataFile.exists()) {
        LOG.error("{} don't exist!", dataFileName);
        continue;
      }

      File indexFile = new File(indexFileName);
      if (!indexFile.exists()) {
        LOG.error("{} don't exist!", indexFileName);
        continue;
      }

      shuffleFileInfo.getDataFiles().add(dataFile);
      shuffleFileInfo.getIndexFiles().add(indexFile);
      shuffleFileInfo.getPartitions().add(partition);
      shuffleFileInfo.setKey(shuffleInfo.getKey());
      shuffleFileInfo.setSize(shuffleInfo.getSize());
    }
    return shuffleFileInfo;
  }
}
