package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MultiStorageHdfsClientReadHandlerTest extends HdfsTestBase {

  @Test
  public void handlerReadUnCombinedDataTest() {
    try {
      String basePath = HDFS_URI + "test_backup_read";
      Path combinePath = new Path(basePath + "/app1/0/combine");
      fs.mkdirs(combinePath);
      Path partitionPath = new Path(basePath + "/app1/0/1");
      fs.mkdirs(partitionPath);

      Path dataPath = new Path(basePath + "/app1/0/1/2.data");
      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);
      byte[] data = new byte[256];
      new Random().nextBytes(data);
      ByteBuffer buffer = ByteBuffer.allocate(data.length);
      buffer.put(data);
      buffer.flip();
      writer.writeData(buffer);
      writer.close();

      Path indexPath = new Path(basePath + "/app1/0/1/2.index");
      HdfsFileWriter iWriter = new HdfsFileWriter(indexPath, conf);
      List<Integer> somePartitions = Lists.newArrayList();
      somePartitions.add(1);
      List<Long> someSizes = Lists.newArrayList();
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      iWriter.writeHeader(somePartitions, someSizes);
      FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
          1, 0, 256, 256, 1, 1);
      iWriter.writeIndex(segment);
      iWriter.close();

      Path combineDataPath = new Path(basePath + "/app1/0/combine/1.data");
      Path combineIndexPath = new Path(basePath + "/app1/0/combine/1.index");
      HdfsFileWriter combineWriter = new HdfsFileWriter(combineDataPath, conf);
      HdfsFileWriter combineIndexWriter = new HdfsFileWriter(combineIndexPath, conf);

      byte[] data1 = new byte[256];
      new Random().nextBytes(data1);
      ByteBuffer buffer1 = ByteBuffer.allocate(data1.length);
      buffer1.put(data1);
      buffer1.flip();
      combineWriter.writeData(buffer1);

      byte[] data2 = new byte[256];
      new Random().nextBytes(data2);
      ByteBuffer buffer2 = ByteBuffer.allocate(data2.length);
      buffer2.put(data2);
      buffer2.flip();
      combineWriter.writeData(buffer2);

      List<Integer> partitions = Lists.newArrayList();
      partitions.add(2);
      partitions.add(3);
      List<Long> sizes = Lists.newArrayList();
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      combineIndexWriter.writeHeader(partitions, sizes);
      FileBasedShuffleSegment segment1 = new FileBasedShuffleSegment(
          2, 0, 256, 256, 1, 1);
      combineIndexWriter.writeIndex(segment1);
      FileBasedShuffleSegment segment2 = new FileBasedShuffleSegment(
          3, 256, 256, 256, 1, 1);
      combineIndexWriter.writeIndex(segment2);
      combineIndexWriter.close();
      combineWriter.close();

      Set<Long> expects = Sets.newHashSet();
      expects.add(1L);
      List<byte[]> expectData = Lists.newArrayList();
      expectData.add(data);
      compareDataAndIndex("app1", 0, 1, basePath, expectData, 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void handlerReadCombinedDataTest() {
    try {
      String basePath = HDFS_URI + "test_backup_read";
      Path combinePath = new Path(basePath + "/app2/0/combine");
      fs.mkdirs(combinePath);
      Path partitionPath = new Path(basePath + "/app2/0/1");
      fs.mkdirs(partitionPath);
      Path dataPath = new Path(basePath + "/app2/0/1/1.data");

      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);
      byte[] data = new byte[256];
      new Random().nextBytes(data);
      ByteBuffer buffer = ByteBuffer.allocate(data.length);
      buffer.put(data);
      buffer.flip();
      writer.writeData(buffer);
      writer.close();
      Path indexPath = new Path(basePath + "/app2/0/1/1.index");
      HdfsFileWriter iWriter = new HdfsFileWriter(indexPath, conf);
      List<Integer> somePartitions = Lists.newArrayList();
      somePartitions.add(1);
      List<Long> someSizes = Lists.newArrayList();
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      iWriter.writeHeader(somePartitions, someSizes);
      FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
          1, 0, 256, 256, 1, 1);
      iWriter.writeIndex(segment);
      iWriter.close();

      Path combineDataPath = new Path(basePath + "/app2/0/combine/1.data");
      Path combineIndexPath = new Path(basePath + "/app2/0/combine/1.index");
      HdfsFileWriter combineWriter = new HdfsFileWriter(combineDataPath, conf);
      HdfsFileWriter combineIndexWriter = new HdfsFileWriter(combineIndexPath, conf);

      byte[] data1 = new byte[256];
      new Random().nextBytes(data1);
      ByteBuffer buffer1 = ByteBuffer.allocate(data1.length);
      buffer1.put(data1);
      buffer1.flip();
      combineWriter.writeData(buffer1);

      byte[] data2 = new byte[256];
      new Random().nextBytes(data2);
      ByteBuffer buffer2 = ByteBuffer.allocate(data2.length);
      buffer2.put(data2);
      buffer2.flip();
      combineWriter.writeData(buffer2);

      List<Integer> partitions = Lists.newArrayList();
      partitions.add(2);
      partitions.add(3);
      List<Long> sizes = Lists.newArrayList();
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      combineIndexWriter.writeHeader(partitions, sizes);
      FileBasedShuffleSegment segment1 = new FileBasedShuffleSegment(
          2, 0, 256, 256, 2, 1);
      combineIndexWriter.writeIndex(segment1);
      FileBasedShuffleSegment segment2 = new FileBasedShuffleSegment(
          3, 256, 256, 256, 3, 1);
      combineIndexWriter.writeIndex(segment2);
      combineIndexWriter.close();
      combineWriter.close();

      Set<Long> expects = Sets.newHashSet();
      expects.add(2L);
      List<byte[]> expectData = Lists.newArrayList();
      expectData.add(data1);
      compareDataAndIndex("app2", 0, 2, basePath, expectData, 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void handlerReadTwoKindsDataTest() {
    try {
      String basePath = HDFS_URI + "test_backup_read";
      Path combinePath = new Path(basePath + "/app4/0/combine");
      fs.mkdirs(combinePath);
      Path partitionPath = new Path(basePath + "/app4/0/2");
      fs.mkdirs(partitionPath);
      Path dataPath = new Path(basePath + "/app4/0/2/2.data");

      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);
      byte[] data = new byte[256];
      new Random().nextBytes(data);
      ByteBuffer buffer = ByteBuffer.allocate(data.length);
      buffer.put(data);
      buffer.flip();
      writer.writeData(buffer);
      writer.close();
      Path indexPath = new Path(basePath + "/app4/0/2/2.index");
      HdfsFileWriter iWriter = new HdfsFileWriter(indexPath, conf);
      List<Integer> somePartitions = Lists.newArrayList();
      somePartitions.add(2);
      List<Long> someSizes = Lists.newArrayList();
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      iWriter.writeHeader(somePartitions, someSizes);
      FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
          1, 0, 256, 256, 1, 1);
      iWriter.writeIndex(segment);
      iWriter.close();

      Path combineDataPath = new Path(basePath + "/app4/0/combine/1.data");
      Path combineIndexPath = new Path(basePath + "/app4/0/combine/1.index");
      HdfsFileWriter combineWriter = new HdfsFileWriter(combineDataPath, conf);
      HdfsFileWriter combineIndexWriter = new HdfsFileWriter(combineIndexPath, conf);

      byte[] data1 = new byte[256];
      new Random().nextBytes(data1);
      ByteBuffer buffer1 = ByteBuffer.allocate(data1.length);
      buffer1.put(data1);
      buffer1.flip();
      combineWriter.writeData(buffer1);

      byte[] data2 = new byte[256];
      new Random().nextBytes(data2);
      ByteBuffer buffer2 = ByteBuffer.allocate(data2.length);
      buffer2.put(data2);
      buffer2.flip();
      combineWriter.writeData(buffer2);

      List<Integer> partitions = Lists.newArrayList();
      partitions.add(2);
      partitions.add(3);
      List<Long> sizes = Lists.newArrayList();
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      sizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      combineIndexWriter.writeHeader(partitions, sizes);
      FileBasedShuffleSegment segment1 = new FileBasedShuffleSegment(
          2, 0, 256, 256, 2, 1);
      combineIndexWriter.writeIndex(segment1);
      FileBasedShuffleSegment segment2 = new FileBasedShuffleSegment(
          3, 256, 256, 256, 3, 1);
      combineIndexWriter.writeIndex(segment2);
      combineIndexWriter.close();
      combineWriter.close();

      Set<Long> expects = Sets.newHashSet();
      expects.add(2L);
      List<byte[]> expectData = Lists.newArrayList();
      expectData.add(data1);
      expectData.add(data);
      compareDataAndIndex("app4", 0, 2, basePath, expectData, 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void readWithDifferentLimitTest() {
    try {
      List<byte[]> expectData = Lists.newArrayList();
      String basePath = HDFS_URI + "test_limit_read";
      Path partitionPath = new Path(basePath + "/app3/0/combine");
      fs.mkdirs(partitionPath);
      Path dataPath = new Path(basePath + "/app3/0/combine/1.data");
      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);

      byte[] data = new byte[256];
      new Random().nextBytes(data);
      ByteBuffer buffer = ByteBuffer.allocate(data.length);
      buffer.put(data);
      buffer.flip();
      writer.writeData(buffer);
      for (int i = 0; i < 5; i++) {
        new Random().nextBytes(data);
        buffer = ByteBuffer.allocate(data.length);
        buffer.put(data);
        buffer.flip();
        expectData.add(data.clone());
        writer.writeData(buffer);
      }
      buffer = ByteBuffer.allocate(data.length);
      new Random().nextBytes(data);
      buffer.put(data);
      buffer.flip();
      writer.writeData(buffer);
      writer.close();

      Path indexPath = new Path(basePath + "/app3/0/combine/1.index");
      HdfsFileWriter iWriter = new HdfsFileWriter(indexPath, conf);
      List<Integer> somePartitions = Lists.newArrayList();
      somePartitions.add(0);
      somePartitions.add(1);
      somePartitions.add(2);
      List<Long> someSizes = Lists.newArrayList();
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE * 5);
      someSizes.add((long)FileBasedShuffleSegment.SEGMENT_SIZE);
      iWriter.writeHeader(somePartitions, someSizes);
      FileBasedShuffleSegment segment = new FileBasedShuffleSegment(1, 0, 256, 256, 1, 1L);
      iWriter.writeIndex(segment);
      for (int i = 0; i < 5; i++) {
        segment = new FileBasedShuffleSegment(i + 2, i * 256, 256, 256, 1, 1L);
        iWriter.writeIndex(segment);
      }
      segment = new FileBasedShuffleSegment(7, 0, 256, 256, 1, 1L);
      iWriter.writeIndex(segment);
      iWriter.close();
      compareDataAndIndex("app3", 0, 1, basePath, expectData, 1);
      compareDataAndIndex("app3", 0, 1, basePath, expectData, 2);
      compareDataAndIndex("app3", 0, 1, basePath, expectData, 3);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void compareDataAndIndex(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      List<byte[]> expectedData,
      int limit) throws IllegalStateException {
    // read directly and compare
    MultiStorageHdfsClientReadHandler handler = new MultiStorageHdfsClientReadHandler(appId,
        shuffleId, partitionId, limit, 1, 3, 1024,
        basePath, conf);
    try {
      List<ByteBuffer> actual = readData(handler);
      compareBytes(expectedData, actual);
    } finally {
      handler.close();
    }
  }

  private List<ByteBuffer> readData(MultiStorageHdfsClientReadHandler handler) throws IllegalStateException {
    ShuffleDataResult sdr;
    List<ByteBuffer> result = Lists.newArrayList();
    int index = 0;
    do {
      sdr = handler.readShuffleData(index);
      if (sdr == null || sdr.getData() == null) {
        break;
      }
      List<BufferSegment> bufferSegments = sdr.getBufferSegments();
      for (BufferSegment bs : bufferSegments) {
        byte[] data = new byte[bs.getLength()];
        System.arraycopy(sdr.getData(), bs.getOffset(), data, 0, bs.getLength());
        result.add(ByteBuffer.wrap(data));
      }
      index++;
    } while(sdr.getData() != null);
    return result;
  }

  private void compareBytes(List<byte[]> expected, List<ByteBuffer> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      byte[] expectedI = expected.get(i);
      ByteBuffer bb = actual.get(i);
      for (int j = 0; j < expectedI.length; j++) {
        assertEquals(expectedI[j], bb.get(j));
      }
    }
  }
}