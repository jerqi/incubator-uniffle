package com.tencent.rss.client.util;

import com.tencent.rss.common.util.Constants;

public class ClientUtils {

  // BlockId is long and composed by partitionId, executorId and AtomicInteger
  // partitionId is first 19 bit, max value is 2^19 - 1 = 524287
  // taskAttemptId is next 25 bit, max value is 2^25 - 1 = 33554431
  // AtomicInteger is rest of 19 bit, max value is 2^19 - 1 = 524287
  public static Long getBlockId(long partitionId, long taskAttemptId, int atomicInt) {
    if (atomicInt < 0 || atomicInt > Constants.MAX_BLOCK_ID) {
      throw new RuntimeException("Can't support blockId[" + atomicInt
          + "], the max value should be " + Constants.MAX_BLOCK_ID);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new RuntimeException("Can't support partitionId["
          + partitionId + "], the max value should be " + Constants.MAX_PARTITION_ID);
    }
    if (taskAttemptId < 0 || taskAttemptId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RuntimeException("Can't support taskAttemptId["
          + taskAttemptId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (partitionId << (Constants.ATOMIC_INT_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (taskAttemptId << Constants.ATOMIC_INT_MAX_LENGTH) + atomicInt;
  }

  // blockId will be stored in bitmap in shuffle server, more bitmap will cost more memory
  // to reduce memory cost, merge blockId of different partition in one bitmap
  public static int getBitmapNum(
      int taskNum,
      int partitionNum,
      int blockNumPerTaskPerPartition,
      long blockNumPerBitmap) {
    // depend on performance test, spark.rss.block.per.bitmap should be great than 20000000
    if (blockNumPerBitmap < 20000000) {
      throw new IllegalArgumentException("blockNumPerBitmap should be greater than 20000000");
    }
    // depend on actual job, spark.rss.block.per.task.partition should be less than 1000000
    // which maybe generate about 1T shuffle data/per task per partition
    if (blockNumPerTaskPerPartition < 0 || blockNumPerTaskPerPartition > 1000000) {
      throw new IllegalArgumentException("blockNumPerTaskPerPartition should be less than 1000000");
    }
    // to avoid overflow when do the calculation, reduce the data if possible
    // it's ok the result is not accuracy
    int processedTaskNum = taskNum;
    int processedPartitionNum = partitionNum;
    long processedBlockNumPerBitmap = blockNumPerBitmap;
    if (taskNum > 1000) {
      processedTaskNum = taskNum / 1000;
      processedBlockNumPerBitmap = processedBlockNumPerBitmap / 1000;
    }
    if (partitionNum > 1000) {
      processedPartitionNum = partitionNum / 1000;
      processedBlockNumPerBitmap = processedBlockNumPerBitmap / 1000;
    }
    long bitmapNum = 1L * blockNumPerTaskPerPartition * processedTaskNum
        * processedPartitionNum / processedBlockNumPerBitmap + 1;
    if (bitmapNum > partitionNum || bitmapNum < 0) {
      bitmapNum = partitionNum;
    }
    return (int) bitmapNum;
  }
}
