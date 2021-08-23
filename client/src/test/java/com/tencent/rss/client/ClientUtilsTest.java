package com.tencent.rss.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.tencent.rss.client.util.ClientUtils;
import org.junit.Test;

public class ClientUtilsTest {

  private static String EXCEPTION_EXPECTED = "Exception excepted";

  @Test
  public void getBlockIdTest() {
    // max value of blockId
    assertEquals(
        new Long(9223372036854775807L), ClientUtils.getBlockId(524287, 33554431, 524287));
    // just a random test
    assertEquals(
        new Long(1759218656870500L), ClientUtils.getBlockId(100, 100, 100));
    // min value of blockId
    assertEquals(
        new Long(0L), ClientUtils.getBlockId(0, 0, 0));
    try {
      ClientUtils.getBlockId(524288, 0, 0);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support partitionId[524288], the max value should be 524287"));
    }
    try {
      ClientUtils.getBlockId(0, 33554432, 0);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support taskAttemptId[33554432], the max value should be 33554431"));
    }
    try {
      ClientUtils.getBlockId(0, 0, 524288);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support blockId[524288], the max value should be 524287"));
    }
  }

  @Test
  public void getBitmapNumTest() {
    // max value of taskNum, partitionNum, blockNumPerTaskPerPartition, it is unexpected in real job
    assertEquals(
        2147483647, ClientUtils.getBitmapNum(Integer.MAX_VALUE, Integer.MAX_VALUE, 1000000, 100000000L));
    // taskNum * partitionNum * blockNumPerTaskPerPartition / blockNumPerBitmap > 0
    assertEquals(
        5001, ClientUtils.getBitmapNum(100000, 100000, 50, 100000000L));
    // taskNum * partitionNum * blockNumPerTaskPerPartition / blockNumPerBitmap = 0
    assertEquals(
        1, ClientUtils.getBitmapNum(1999, 1999, 50, 100000000L));
    try {
      ClientUtils.getBitmapNum(1, 1, 1, 19999999L);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("blockNumPerBitmap should be greater than"));
    }
    try {
      ClientUtils.getBitmapNum(1, 1, 1000001, 20000000L);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("blockNumPerTaskPerPartition should be less than"));
    }
  }
}
