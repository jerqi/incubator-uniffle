package com.tencent.rss.common.util;

public class Constants {

  public static final String SHUFFLE_DATA_FILE_SUFFIX = ".data";
  public static final String SHUFFLE_INDEX_FILE_SUFFIX = ".index";
  // BlockId is long and consist of partitionId, taskAttemptId, atomicInt
  // the length of them are 19 + 25 + 19 = 63
  public static final int PARTITION_ID_MAX_LENGTH = 19;
  public static final int TASK_ATTEMPT_ID_MAX_LENGTH = 25;
  public static final int ATOMIC_INT_MAX_LENGTH = 19;
  public static int MAX_BLOCK_ID = (1 << Constants.ATOMIC_INT_MAX_LENGTH) - 1;
  public static long MAX_PARTITION_ID = (1 << Constants.PARTITION_ID_MAX_LENGTH) - 1;
  public static long MAX_TASK_ATTEMPT_ID = (1 << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) - 1;
}
