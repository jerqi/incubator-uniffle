package com.tencent.rss.client.response;

public class CompressedShuffleBlock {

  private byte[] compressData;
  private int uncompressLength;

  public CompressedShuffleBlock(byte[] compressData, int uncompressLength) {
    this.compressData = compressData;
    this.uncompressLength = uncompressLength;
  }

  public byte[] getCompressData() {
    return compressData;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }
}
