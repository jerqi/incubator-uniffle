package com.tencent.rss.common.util;

public class Constants {

  // the value is used for client/server compatible, eg, online upgrade
  public static final String SHUFFLE_SERVER_VERSION = "ss_v2";
  public static final String SHUFFLE_DATA_FILE_SUFFIX = ".data";
  public static final String SHUFFLE_INDEX_FILE_SUFFIX = ".index";
  // BlockId is long and consist of partitionId, taskAttemptId, atomicInt
  // the length of them are ATOMIC_INT_MAX_LENGTH + PARTITION_ID_MAX_LENGTH + TASK_ATTEMPT_ID_MAX_LENGTH = 63
  public static final int PARTITION_ID_MAX_LENGTH = 24;
  public static final int TASK_ATTEMPT_ID_MAX_LENGTH = 20;
  public static final int ATOMIC_INT_MAX_LENGTH = 19;
  public static long MAX_SEQUENCE_NO = (1 << Constants.ATOMIC_INT_MAX_LENGTH) - 1;
  public static long MAX_PARTITION_ID = (1 << Constants.PARTITION_ID_MAX_LENGTH) - 1;
  public static long MAX_TASK_ATTEMPT_ID = (1 << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) - 1;
}
